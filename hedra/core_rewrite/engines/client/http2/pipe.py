import asyncio
from typing import (
    Dict,
    List,
    Optional,
    Tuple,
)

from .config import H2Configuration
from .errors import (
    ErrorCodes,
    StreamClosedError,
    StreamError,
)
from .events import (
    ConnectionTerminated,
    DataReceived,
    StreamReset,
    WindowUpdated,
)
from .fast_hpack import Decoder, Encoder, HeaderTable
from .frames.types.base_frame import Frame
from .protocols import HTTP2Connection
from .settings import SettingCodes, Settings, StreamClosedBy
from .windows import WindowManager


class HTTP2Pipe:

    CONFIG = H2Configuration(
        validate_inbound_headers=False,
    )

    def __init__(self, concurrency):

        self.connected = False
        self.concurrency = concurrency
        self._encoder = Encoder()
        self._decoder = Decoder()
        self._decoder.header_table = HeaderTable()
        self._decoder.max_allowed_table_size = self._decoder.header_table.maxsize
        self._init_sent = False
        self._data_to_send = b''
        self._headers_sent = False
        self.lock = asyncio.Lock()

        self.local_settings = Settings(
            client=True,
            initial_values={
                SettingCodes.ENABLE_PUSH: 0,
                SettingCodes.MAX_CONCURRENT_STREAMS: concurrency,
                SettingCodes.MAX_HEADER_LIST_SIZE: 65535,
            }
        )
        self.remote_settings = Settings(
            client=False
        )

        self.outbound_flow_control_window = self.remote_settings.initial_window_size

        del self.local_settings[SettingCodes.ENABLE_CONNECT_PROTOCOL]

        self._inbound_flow_control_window_manager = WindowManager(
            max_window_size=self.local_settings.initial_window_size
        )

        self.local_settings_dict = {setting_name: setting_value for setting_name, setting_value in self.local_settings.items()}
        self.remote_settings_dict = {setting_name: setting_value for setting_name, setting_value in self.remote_settings.items()}


    def _guard_increment_window(self, current, increment):
        # The largest value the flow control window may take.
        LARGEST_FLOW_CONTROL_WINDOW = 2**31 - 1

        new_size = current + increment

        if new_size > LARGEST_FLOW_CONTROL_WINDOW:
            self.outbound_flow_control_window = (
                self.remote_settings.initial_window_size
            )

            self._inbound_flow_control_window_manager = WindowManager(
                max_window_size=self.local_settings.initial_window_size
            )

        return LARGEST_FLOW_CONTROL_WINDOW - current
    
    def send_preamble(self, connection: HTTP2Connection): 

        if self._init_sent is False:

            window_increment = 65536

            self._inbound_flow_control_window_manager.window_opened(window_increment)

            connection.write(b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n')
            self._init_sent = True
            
            self.outbound_flow_control_window = self.remote_settings.initial_window_size

        return connection

    def send_request_headers(
            self,                 
            headers: List[Tuple[bytes, bytes]],
            data: Optional[bytes], 
            connection: HTTP2Connection
        ):

        connection.stream.inbound = WindowManager(self.local_settings.initial_window_size)
        connection.stream.outbound = WindowManager(self.remote_settings.initial_window_size)
        connection.stream.max_inbound_frame_size = self.local_settings.max_frame_size
        connection.stream.max_outbound_frame_size = self.remote_settings.max_frame_size
        connection.stream.current_outbound_window_size = self.remote_settings.initial_window_size
        
        headers_frame = Frame(connection.stream.stream_id, 0x01)
        headers_frame.flags.add('END_HEADERS')

        encoded_headers = self._encoder.encode(headers)
        encoded_headers = [
            encoded_headers[i:i+self.remote_settings.max_frame_size]
            for i in range(
                0, len(encoded_headers), self.remote_settings.max_frame_size
            )
        ]
    
        headers_frame.data = encoded_headers[0]
    
        if data is None:
            headers_frame.flags.add('END_STREAM')

        connection.stream.inbound.window_opened(65536) 
  
        connection.write(headers_frame.serialize())

        return connection

    async def receive_response(
        self, 
        connection: HTTP2Connection
    ):

        body_data = bytearray()
        status_code: Optional[int] = 200
        headers_dict: Dict[bytes, bytes] = {}
        error: Optional[Exception] = None

        done = False
        while done is False:

            data = b''

            data = await connection.read()

            if data == b'':
                done = True

            connection.stream.frame_buffer.data.extend(data)
            connection.stream.frame_buffer.max_frame_size = connection.stream.max_outbound_frame_size

            write_data = bytearray()
            frames = None
            stream_events: List[Frame] = []

            for frame in connection.stream.frame_buffer:
                try:

                    if frame.type == 0x0:
                        # DATA

                        end_stream = 'END_STREAM' in frame.flags
                        flow_controlled_length = frame.flow_controlled_length
                        frame_data = frame.data

                        frames = []
                        self._inbound_flow_control_window_manager.window_consumed(
                            flow_controlled_length
                        )

                        try:
                            
                            connection.stream.inbound.window_consumed(flow_controlled_length)
                    
                            event = DataReceived()
                            event.stream_id = connection.stream.stream_id

                            stream_events.append(event)

                            if end_stream:
                                done = True
                                

                            stream_events[0].data = frame_data
                            stream_events[0].flow_controlled_length = flow_controlled_length

                        except StreamClosedError as e:
                            status_code = status_code or 400
                            error = Exception(f'Connection - {connection.stream.stream_id} err: {str(e._events[0])}')

                    elif frame.type == 0x07:
                        # GOAWAY
                        self._data_to_send = b''

                        new_event = ConnectionTerminated()
                        new_event.error_code = ErrorCodes(frame.error_code)
                        new_event.last_stream_id = frame.last_stream_id
                        
                        if frame.additional_data:
                            new_event.additional_data = frame.additional_data

                        frames = []
                        done = True

                    elif frame.type == 0x01:
                        # HEADERS
                        headers: List[Tuple[bytes, bytes]] = {}

                        try:
                            headers = self._decoder.decode(frame.data, raw=True)

                        except Exception as headers_read_err:
                            status_code = status_code or 400
                            error = headers_read_err

                        for k, v in headers:
                            if k == ":status":
                                status_code = int(v)
                            elif k.startswith(":"):
                                headers_dict[k.strip(':')] = v
                            else:
                                headers_dict[k] = v

                        if 'END_STREAM' in frame.flags:
                            done = True

                        frames = []

                    elif frame.type == 0x03:
                        # RESET

                        self.closed_by = StreamClosedBy.RECV_RST_STREAM
                        reset_event = StreamReset()
                        reset_event.stream_id = connection.stream.stream_id

                        reset_event.error_code = ErrorCodes(frame.error_code)

                        status_code = 400
                        error = Exception(f'Connection - {connection.stream.stream_id} - err: {str(reset_event)}')

                    elif frame.type == 0x04:
                        # SETTINGS

                        if 'ACK' in frame.flags:

                            changes = self.local_settings.acknowledge()
                    
                            initial_window_size_change = changes.get(SettingCodes.INITIAL_WINDOW_SIZE)
                            max_header_list_size_change = changes.get(SettingCodes.MAX_HEADER_LIST_SIZE)
                            max_frame_size_change = changes.get(SettingCodes.MAX_FRAME_SIZE)
                            header_table_size_change = changes.get(SettingCodes.HEADER_TABLE_SIZE)

                            if initial_window_size_change is not None:

                                window_delta = initial_window_size_change.new_value - initial_window_size_change.original_value
                                
                                new_max_window_size = connection.stream.inbound.max_window_size + window_delta
                                connection.stream.inbound.window_opened(window_delta)
                                connection.stream.inbound.max_window_size = new_max_window_size

                            if max_header_list_size_change is not None:
                                self._decoder.max_header_list_size = max_header_list_size_change.new_value

                            if max_frame_size_change is not None:
                                self.max_outbound_frame_size =  max_frame_size_change.new_value

                            if header_table_size_change:
                                # This is safe across all hpack versions: some versions just won't
                                # respect it.
                                self._decoder.max_allowed_table_size = header_table_size_change.new_value

                        # Add the new settings.
                        self.remote_settings.update(frame.settings)
           
                        changes = self.remote_settings.acknowledge()
                        initial_window_size_change = changes.get(SettingCodes.INITIAL_WINDOW_SIZE)
                        header_table_size_change = changes.get(SettingCodes.HEADER_TABLE_SIZE)
                        max_frame_size_change = changes.get(SettingCodes.MAX_FRAME_SIZE)
                    
                        if initial_window_size_change:
                            self.current_outbound_window_size = self._guard_increment_window(
                                self.current_outbound_window_size,
                                initial_window_size_change.new_value - initial_window_size_change.original_value
                            )

                        # HEADER_TABLE_SIZE changes by the remote part affect our encoder: cf.
                        # RFC 7540 Section 6.5.2.
                        if  header_table_size_change:
                            self._encoder.header_table_size = header_table_size_change.new_value

                        if max_frame_size_change:
                            self.max_outbound_frame_size = max_frame_size_change.new_value

                        frame = Frame(0, 0x04)
                        frame.flags.add('ACK')

                        frames = [frame]

                    elif frame.type == 0x08:
                        # WINDOW UPDATE

                        frames = []
                        increment = frame.window_increment
                        if frame.stream_id:
                            try:

                                
                                event = WindowUpdated()
                                event.stream_id = connection.stream.stream_id

                                # If we encounter a problem with incrementing the flow control window,
                                # this should be treated as a *stream* error, not a *connection* error.
                                # That means we need to catch the error and forcibly close the stream.
                                event.delta = increment

                                try:
                                    self.outbound_flow_control_window = self._guard_increment_window(
                                        self.outbound_flow_control_window,
                                        increment
                                    )
                                except StreamError:
                                    # Ok, this is bad. We're going to need to perform a local
                                    # reset.

                                    event = StreamReset()
                                    event.stream_id = connection.stream.stream_id
                                    event.error_code = ErrorCodes.FLOW_CONTROL_ERROR
                                    event.remote_reset = False

                                    self.closed_by = ErrorCodes.FLOW_CONTROL_ERROR    

                                    status_code = 400
                                    error = Exception(f'Connection - {connection.stream.stream_id} err: {str(event)}')

                            except Exception:
                                frames = []
                                
                        else:
                            self.outbound_flow_control_window = self._guard_increment_window(
                                self.outbound_flow_control_window,
                                increment
                            )
                            # FIXME: Should we split this into one event per active stream?
                            window_updated_event = WindowUpdated()
                            window_updated_event.stream_id = 0
                            window_updated_event.delta = increment

                            frames = []

                except Exception as e:
                    status_code = status_code or 400
                    error = Exception(f'Connection - {connection.stream.stream_id} err- {str(e)}')

                if frames:
                    for f in frames:
                        write_data.extend(f.serialize())

                    connection.write(write_data)

            for event in stream_events:
                amount = event.flow_controlled_length

                conn_increment = self._inbound_flow_control_window_manager.process_bytes(amount)

                if conn_increment:
                    connection.stream.write_window_update_frame(
                        stream_id=0, 
                        window_increment=conn_increment
                    )

                if event.data is None:
                    event.data = b''

                body_data.extend(event.data)

            if done:
                break

        return (
            status_code,
            headers_dict,
            bytes(body_data),
            error
        )

    async def submit_request_body(
        self, 
        data: bytes,
        connection: HTTP2Connection
    ):
        
        while data:
            local_flow = self.current_outbound_window_size
            max_frame_size = self.max_outbound_frame_size
            flow = min(local_flow, max_frame_size)
            while flow == 0:
                await self.receive_response(connection)

                local_flow = self.current_outbound_window_size
                max_frame_size = self.max_outbound_frame_size
                flow = min(local_flow, max_frame_size)
                
            max_flow = flow
            chunk_size = min(len(data), max_flow)
            chunk, data = data[:chunk_size], data[chunk_size:]

            df = Frame(connection.stream.stream_id, 0x0)
            df.data = chunk

            # Subtract flow_controlled_length to account for possible padding
            self.outbound_flow_control_window -= df.flow_controlled_length
            assert self.outbound_flow_control_window >= 0

            connection.write(df.serialize())

        df = Frame(connection.stream.stream_id, 0x0)
        df.flags.add('END_STREAM')

        connection.write(df.serialize())

        return connection
