import asyncio
import h2.stream
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
import h2.errors
from hyperframe.frame import SettingsFrame
from hedra.core.engines.types.http2.reader_writer import ReaderWriter
from hedra.core.engines.types.common import Response, Request
from hedra.core.engines.types.common.encoder import Encoder
from hedra.core.engines.types.common.decoder import Decoder
from .windows import WindowManager
from .frames.types import *


class HTTP2Connection:
    CONFIG = h2.config.H2Configuration(
        validate_inbound_headers=False
    )

    def __init__(self, concurrency):
        self.loop = asyncio.get_running_loop()
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

    async def _receive_events(self, stream: ReaderWriter) -> None:
        
        data = await stream.read()

        stream.frame_buffer.add_data(data)
        stream.frame_buffer.max_frame_size = stream.max_outbound_frame_size

        events = []

        for frame in stream.frame_buffer:
            try:
                frames, stream_events = frame.get_events_and_frames(stream, self)
                if stream_events:
                    events.extend(stream_events)

            except Exception as e:
                raise Exception(f'Connection {stream.stream_id} err- {str(e)}')

            if frames:        
                self._data_to_send += b''.join(f.serialize() for f in frames)

        stream.write(self._data_to_send)
        self._data_to_send = b''

        return events

    def connect(self, stream: ReaderWriter):

        self._headers_sent = False

        if self._init_sent is False or stream.reset_connection:
            self._data_to_send = b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'

            f = SettingsFrame(0)
            f.settings = self.local_settings_dict
            self._data_to_send += f.serialize()

            self._inbound_flow_control_window_manager.window_opened(65536)
            f = WindowUpdateFrame(0)
            f.window_increment = 65536
            self._data_to_send += f.serialize()

            stream.write(self._data_to_send)
            self._data_to_send = b''
            self._init_sent = True

            self.outbound_flow_control_window = self.remote_settings.initial_window_size

        stream.inbound = WindowManager(self.local_settings.initial_window_size)
        stream.outbound = WindowManager(self.remote_settings.initial_window_size)
        stream.max_inbound_frame_size = self.local_settings.max_frame_size
        stream.max_outbound_frame_size = self.remote_settings.max_frame_size
        stream.current_outbound_window_size = self.remote_settings.initial_window_size

    async def receive_response(self, response: Response, stream: ReaderWriter):
       
        done = False
        while done is False:
            events = await self._receive_events(stream)
            for event in events:

                event_type = event.event_type

                if event.error_code is not None:
                    raise Exception(f'Connection - {stream.stream_id} err: {str(event)}')

                elif event_type == 'DEFERRED_HEADERS':
                    response.deferred_headers = event

                elif event_type == 'DATA_RECEIVED':
                    amount = event.flow_controlled_length
                    frames = []
                    conn_increment = self._inbound_flow_control_window_manager.process_bytes(amount)
                    if conn_increment:
                        f = WindowUpdateFrame(0)
                        f.window_increment = conn_increment
                        frames.append(f)

                    increment = stream.inbound.process_bytes(amount)
      
                    if increment:
                        f = WindowUpdateFrame(stream.stream_id)
                        f.window_increment = increment
                        frames.append(f)

                    self._data_to_send += b''.join([frame.serialize() for frame in frames])
                    stream.write(self._data_to_send)
                    self._data_to_send = b''

                    response.body += event.data

                elif event_type== 'STREAM_ENDED' or event_type == 'STREAM_RESET':
                    done = True
                    break

        return response

    def send_request_headers(self, request: Request, stream: ReaderWriter) -> None:
        end_stream = request.payload.has_data is False
        encoded_headers = request.headers.encoded_headers

        if request.headers.hpack_encoder.header_table.maxsize != self._encoder.header_table.maxsize:
            request.headers.hpack_encoder.header_table_size = self._encoder.header_table.maxsize
            request.headers.setup_http2_headers(
                request.method,
                request.url
            )

        header_blocks = [
            encoded_headers[i:i+stream.max_outbound_frame_size]
            for i in range(
                0, len(encoded_headers), stream.max_outbound_frame_size
            )
        ]

        headers_frame = HeadersFrame(stream.stream_id)

        frames = []
        headers_frame.data = header_blocks[0]
        if end_stream:
            headers_frame.flags.add('END_STREAM')

        frames.append(headers_frame)

        for block in header_blocks[1:]:
            cf = ContinuationFrame(stream.stream_id)
            cf.data = block
            frames.append(cf)

        frames[-1].flags.add('END_HEADERS')

        stream._authority = request.headers.data.get('authority')

        self.request_method = request.method

        window_size = 65536
        stream.inbound.window_opened(window_size)

        window_update_frame = WindowUpdateFrame(stream.stream_id)
        window_update_frame.window_increment = window_size

        frames.append(window_update_frame)
        
        stream.write(
            b''.join([frame.serialize() for frame in frames ])
        )
        self._data_to_send = b''

        self._headers_sent = True

    async def submit_request_body(self, request: Request, stream: ReaderWriter) -> None:
        if request.payload.has_data:
            data = request.payload.encoded_data
            
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
                self._data_to_send = b''

            df = DataFrame(stream.stream_id)
            df.flags.add('END_STREAM')

            stream.write(df.serialize())
            self._data_to_send = b''