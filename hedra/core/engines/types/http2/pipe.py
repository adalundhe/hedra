import asyncio
import traceback
import h2.stream
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
import h2.errors
from hedra.core.engines.types.http2.stream import Stream
from hedra.core.engines.types.common.encoder import Encoder
from hedra.core.engines.types.common.decoder import Decoder
from hedra.core.engines.types.http2.events.remote_settings_changed_event import RemoteSettingsChanged
from hedra.core.engines.types.http2.events.settings_acknowledged_event import SettingsAcknowledged
from hedra.core.engines.types.http2.streams.stream_settings import SettingCodes
from hedra.core.engines.types.http2.events.stream_reset import StreamReset
from hedra.core.engines.types.http2.streams.stream_closed_by import StreamClosedBy
from hedra.core.engines.types.http2.events.deferred_headers_event import DeferredHeaders
from hedra.core.engines.types.http2.events.connection_terminated_event import ConnectionTerminated
from hedra.core.engines.types.http2.errors.exceptions import StreamClosedError
from hedra.core.engines.types.http2.events.data_received_event import DataReceived
from hedra.core.engines.types.http2.errors.exceptions import StreamError
from hedra.core.engines.types.http2.errors.types import ErrorCodes
from hedra.core.engines.types.http2.events.window_updated_event import WindowUpdated
from .action import HTTP2Action
from .result import HTTP2Result
from .windows import WindowManager
from .frames.types.base_frame import Frame


class HTTP2Pipe:
    __slots__ = (
        '_h2_state',
        'connected',
        'concurrency',
        '_encoder',
        '_decoder',
        '_init_sent',
        'stream_id',
        '_data_to_send',
        '_headers_sent',
        'lock',
        'local_settings',
        'remote_settings',
        'outbound_flow_control_window',
        'local_settings',
        '_inbound_flow_control_window_manager',
        'local_settings_dict',
        'remote_settings_dict'
    )

    CONFIG = h2.config.H2Configuration(
        validate_inbound_headers=False,
    )

    def __init__(self, concurrency):
        self._h2_state = h2.connection.H2Connection(config=self.CONFIG)
        self.connected = False
        self.concurrency = concurrency
        self._encoder = Encoder()
        self._decoder = Decoder()
        self._init_sent = False
        self.stream_id = None
        self._data_to_send = b''
        self._headers_sent = False
        self.lock = asyncio.Lock()

        self.local_settings = h2.settings.Settings(
            client=True,
            initial_values={
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: concurrency,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 65535,
            }
        )
        self.remote_settings = h2.settings.Settings(
            client=False
        )

        self.outbound_flow_control_window = self.remote_settings.initial_window_size

        del self.local_settings[h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL]

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

    def send_request_headers(self, request: HTTP2Action, stream: Stream):

        self._headers_sent = False

        if self._init_sent is False or stream.reset_connection:

            window_increment = 65536

            self._inbound_flow_control_window_manager.window_opened(window_increment)

            stream.write(bytes(stream.connection_data))
            self._init_sent = True

            self.outbound_flow_control_window = self.remote_settings.initial_window_size

        stream.inbound = WindowManager(self.local_settings.initial_window_size)
        stream.outbound = WindowManager(self.remote_settings.initial_window_size)
        stream.max_inbound_frame_size = self.local_settings.max_frame_size
        stream.max_outbound_frame_size = self.remote_settings.max_frame_size
        stream.current_outbound_window_size = self.remote_settings.initial_window_size

        end_stream = request.encoded_data is None
        encoded_headers = request.encoded_headers

        if request.hpack_encoder.header_table.maxsize != self._encoder.header_table.maxsize:
            request.hpack_encoder.header_table_size = self._encoder.header_table.maxsize
            request._setup_headers()
    
        stream.headers_frame.data = encoded_headers[0]
        headers_frame = stream.headers_frame
        if end_stream:
            headers_frame.flags.add('END_STREAM')

        stream.inbound.window_opened(65536) 
  
        stream.write(headers_frame.serialize())
        self._headers_sent = True

    async def receive_response(self, response: HTTP2Result, stream: Stream):

        done = False
        while done is False:
            
            data = await asyncio.wait_for(stream.read(), timeout=stream.timeouts.socket_read_timeout)

            stream.frame_buffer.data.extend(data)
            stream.frame_buffer.max_frame_size = stream.max_outbound_frame_size

            write_data = bytearray()
            frames = None
            stream_events = []

            for frame in stream.frame_buffer:
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
                            
                            stream.inbound.window_consumed(flow_controlled_length)
                    
                            event = DataReceived()
                            event.stream_id = stream.stream_id

                            stream_events.append(event)

                            if end_stream:
                                done = True

                            stream_events[0].data = frame_data
                            stream_events[0].flow_controlled_length = flow_controlled_length

                        except StreamClosedError as e:
                            raise Exception(f'Connection - {stream.stream_id} err: {str(e._events[0])}')

                    elif frame.type == 0x07:
                        # GOAWAY
                        self._data_to_send = b''

                        new_event = ConnectionTerminated()
                        new_event.error_code = h2.errors._error_code_from_int(frame.error_code)
                        new_event.last_stream_id = frame.last_stream_id
                        
                        if frame.additional_data:
                            new_event.additional_data = frame.additional_data

                        frames = []
                        raise Exception(f'Connection - {stream.stream_id} err: {str(new_event)}')

                    elif frame.type == 0x01:
                        # HEADERS
                        deferred_headers = DeferredHeaders(
                            stream.encoder,
                            frame,
                            self._h2_state.config.header_encoding
                        )

                        if deferred_headers.end_stream:
                            done = True

                        frames = []
                        response.deferred_headers = deferred_headers

                    elif frame.type == 0x03:
                        # RESET

                        stream.closed_by = StreamClosedBy.RECV_RST_STREAM
                        reset_event = StreamReset()
                        reset_event.stream_id = stream.stream_id

                        reset_event.error_code = h2.errors._error_code_from_int(frame.error_code)

                        raise Exception(f'Connection - {stream.stream_id} err: {str(reset_event)}')

                    elif frame.type == 0x04:
                        # SETTINGS

                        if 'ACK' in frame.flags:

                            changes = self.local_settings.acknowledge()
                    
                            initial_window_size_change = changes.get(SettingCodes.INITIAL_WINDOW_SIZE)
                            max_header_list_size_change = changes.get(SettingCodes.MAX_HEADER_LIST_SIZE)
                            max_frame_size_change = changes.get(SettingCodes.MAX_FRAME_SIZE)
                            header_table_size_change =changes.get(SettingCodes.HEADER_TABLE_SIZE)

                            if initial_window_size_change is not None:

                                window_delta = initial_window_size_change.new_value - initial_window_size_change.original_value
                                
                                new_max_window_size = stream.inbound.max_window_size + window_delta
                                stream.inbound.window_opened(window_delta)
                                stream.inbound.max_window_size = new_max_window_size

                            if max_header_list_size_change is not None:
                                self._decoder.max_header_list_size = max_header_list_size_change.new_value

                            if max_frame_size_change is not None:
                                stream.max_outbound_frame_size =  max_frame_size_change.new_value

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
                            stream.current_outbound_window_size = self._guard_increment_window(
                                stream.current_outbound_window_size,
                                initial_window_size_change.new_value - initial_window_size_change.original_value
                            )

                        # HEADER_TABLE_SIZE changes by the remote part affect our encoder: cf.
                        # RFC 7540 Section 6.5.2.
                        if  header_table_size_change:
                            self._encoder.header_table_size = header_table_size_change.new_value

                        if max_frame_size_change:
                            stream.max_outbound_frame_size = max_frame_size_change.new_value

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
                                event.stream_id = stream.stream_id

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
                                    event.stream_id = stream.stream_id
                                    event.error_code = ErrorCodes.FLOW_CONTROL_ERROR
                                    event.remote_reset = False

                                    stream.closed_by = ErrorCodes.FLOW_CONTROL_ERROR    
                      
                                    raise Exception(f'Connection - {stream.stream_id} err: {str(event)}')
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
                    raise Exception(f'Connection {stream.stream_id} err- {str(e)}')

                if frames:
                    for f in frames:
                        write_data.extend(f.serialize())

                    stream.write(write_data)

            for event in stream_events:
                amount = event.flow_controlled_length

                conn_increment = self._inbound_flow_control_window_manager.process_bytes(amount)

                if conn_increment:
                    stream.write_window_update_frame(0, conn_increment)

                response.body.extend(event.data)

            if done:
                break

        return response

    async def submit_request_body(self, request: HTTP2Action, stream: Stream) -> None:
        data = request.encoded_data
        
        while data:
            local_flow = stream.current_outbound_window_size
            max_frame_size = stream.max_outbound_frame_size
            flow = min(local_flow, max_frame_size)
            while flow == 0:
                await self._receive_events(stream)
                local_flow = stream.current_outbound_window_size
                max_frame_size = stream.max_outbound_frame_size
                flow = min(local_flow, max_frame_size)
                
            max_flow = flow
            chunk_size = min(len(data), max_flow)
            chunk, data = data[:chunk_size], data[chunk_size:]

            df = Frame(stream.stream_id, 0x0)
            df.data = chunk

            # Subtract flow_controlled_length to account for possible padding
            self.outbound_flow_control_window -= df.flow_controlled_length
            assert self.outbound_flow_control_window >= 0

            stream.write(df.serialize())

        df = Frame(stream.stream_id, 0x0)
        df.flags.add('END_STREAM')

        stream.write(df.serialize())