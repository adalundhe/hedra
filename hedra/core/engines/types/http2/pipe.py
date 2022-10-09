import asyncio
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
from .action import HTTP2Action
from .result import HTTP2Result
from .windows import WindowManager
from .frames.types import *


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
            # events = await self._receive_events(stream)
            data = await asyncio.wait_for(stream.read(), timeout=10)

            stream.frame_buffer.data.extend(data)
            stream.frame_buffer.max_frame_size = stream.max_outbound_frame_size

            events = []
            write_data = bytearray()
            for frame in stream.frame_buffer:
                try:
                    frames, stream_events = frame.get_events_and_frames(stream, self)
                    if stream_events:
                        events.extend(stream_events)

                except Exception as e:
                    raise Exception(f'Connection {stream.stream_id} err- {str(e)}')

                if frames:
                    for f in frames:
                        write_data.extend(f.serialize())

                    stream.write(write_data)

            for event in events:
                if event.error_code is not None:
                    raise Exception(f'Connection - {stream.stream_id} err: {str(event)}')

                elif event.event_type == 'DEFERRED_HEADERS':
                    response.deferred_headers = event

                elif event.event_type == 'DATA_RECEIVED':
                    amount = event.flow_controlled_length
    
                    conn_increment = self._inbound_flow_control_window_manager.process_bytes(amount)

                    if conn_increment:
                        stream.write_window_update_frame(0, conn_increment)

                    response.body.extend(event.data)

                elif event.event_type == 'STREAM_ENDED' or event.event_type == 'STREAM_RESET':
                    done = True
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

            df = DataFrame(stream.stream_id)
            df.data = chunk

            # Subtract flow_controlled_length to account for possible padding
            self.outbound_flow_control_window -= df.flow_controlled_length
            assert self.outbound_flow_control_window >= 0

            stream.write(df.serialize())

        df = DataFrame(stream.stream_id)
        df.flags.add('END_STREAM')

        stream.write(df.serialize())