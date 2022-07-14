import asyncio
import h2.stream
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
import h2.errors
from hpack import Encoder, Decoder
from hyperframe.frame import SettingsFrame
from hedra.core.engines.types.http2.reader_writer import ReaderWriter
from hedra.core.engines.types.http2.streams.stream_closed_by import StreamClosedBy
from hedra.core.engines.types.common import Response, Request
from .streams.stream_settings import SettingCodes
from .stream import AsyncStream
from .windows import WindowManager
from .errors import ErrorCodes, StreamError, StreamResetException, StreamClosedError
from .frames import FrameBuffer
from .events import *
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
        self.frame_buffers = {}
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

        self._frame_dispatch_table = {
            'ALTSVC': self._receive_alt_svc_frame,
            'CONTINUATION': self._receive_naked_continuation,
            'DATA': self._receive_data_frame,
            'EXTENSION': self._receive_unknown_frame,
            'GOAWAY': self._receive_goaway_frame,
            'HEADERS': self._receive_headers_frame,
            'PING': self._receive_ping_frame,
            'PRIORITY': self._receive_priority_frame,
            'RESET': self._receive_rst_stream_frame,
            'SETTINGS': self._receive_settings_frame,
            'WINDOWUPDATE': self._receive_window_update_frame
        }

    def _guard_increment_window(self, current, increment):
        """
        Increments a flow control window, guarding against that window becoming too
        large.
        :param current: The current value of the flow control window.
        :param increment: The increment to apply to that window.
        :returns: The new value of the window.
        :raises: ``FlowControlError``
        """
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

    def _receive_headers_frame(self, frame, stream: ReaderWriter):
        
        # Hyper H2 would have you immediate unpack the headers,
        # but as with the Encoder, the Decoder blocks the event loop.
        # Instead, since we know the HeadersFrame is valid, we'll defer
        # parsing the headers until explicitly needed (during results
        # analysis/accumulation).

        stream_events = []

        deferred_headers = DeferredHeaders(
            self._decoder.header_table.maxsize,
            frame,
            self._h2_state.config.header_encoding
        )

        stream_events.append(deferred_headers)

        if deferred_headers.end_stream:
            event = StreamEnded()
            event.stream_id = stream.stream_id
            
            stream_events[0].stream_ended = event
            stream_events.append(event)

        return [], stream_events

    def _receive_settings_frame(self, frame, stream: ReaderWriter):
        events = []
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

            ack_event = SettingsAcknowledged()
            ack_event.changed_settings = changes
            events.append(ack_event)
            return [], events

        # Add the new settings.
        self.remote_settings.update(frame.settings)
        events.append(
            RemoteSettingsChanged.from_settings(
                self.remote_settings, frame.settings
            )
        )

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

        frames = SettingsFrame(0)
        frames.flags.add('ACK')

        return [frames], events

    def _receive_data_frame(self, frame, stream: ReaderWriter):
        end_stream = 'END_STREAM' in frame.flags
        flow_controlled_length = frame.flow_controlled_length
        frame_data = frame.data

        frames = []
        data_events = []
        self._inbound_flow_control_window_manager.window_consumed(
            flow_controlled_length
        )

        try:
            
            stream.inbound.window_consumed(flow_controlled_length)
      
            event = DataReceived()
            event.stream_id = stream.stream_id

            data_events.append(event)

            if end_stream:
                event = StreamEnded()
                event.stream_id = stream.stream_id
                data_events[0].stream_ended = event
                data_events.append(event)

            data_events[0].data = frame_data
            data_events[0].flow_controlled_length = flow_controlled_length
            return frames, data_events

        except StreamClosedError as e:
            # This stream is either marked as CLOSED or already gone from our
            # internal state.
            
            conn_manager = self._inbound_flow_control_window_manager
            conn_increment = conn_manager.process_bytes(
                flow_controlled_length
            )

            if conn_increment:
                f = WindowUpdateFrame(0)
                f.window_increment = conn_increment
                frames.append(f)

            f = RstStreamFrame(e.stream_id)
            f.error_code = e.error_code
            frames.append(f)
    
            return frames, data_events + e._events      
    
    def _receive_window_update_frame(self, frame, stream: ReaderWriter):
        stream_events = []
        frames = []
        increment = frame.window_increment
        if frame.stream_id:
            try:

                
                event = WindowUpdated()
                event.stream_id = self.stream_id

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
                    
                    rsf = RstStreamFrame(stream.stream_id)
                    rsf.error_code = ErrorCodes.FLOW_CONTROL_ERROR

                    frames = [rsf]

                stream_events.append(event)
            except Exception as e:
                return [], stream_events
        else:
            self.outbound_flow_control_window = self._guard_increment_window(
                self.outbound_flow_control_window,
                increment
            )
            # FIXME: Should we split this into one event per active stream?
            window_updated_event = WindowUpdated()
            window_updated_event.stream_id = 0
            window_updated_event.delta = increment
            stream_events.append(window_updated_event)
            frames = []

        return frames, stream_events

    def _receive_ping_frame(self, frame, stream: ReaderWriter):
        return None, None

    def _receive_rst_stream_frame(self, frame, stream: ReaderWriter):

        stream.closed_by = StreamClosedBy.RECV_RST_STREAM
        reset_event = StreamReset()
        reset_event.stream_id = self.stream_id
        reset_event[0].error_code = h2.errors._error_code_from_int(frame.error_code)

        return [], [reset_event]

    def _receive_priority_frame(self, frame, stream: AsyncStream):
        return None, None

    def _receive_goaway_frame(self, frame, stream: AsyncStream):
        self._data_to_send = b''

        new_event = ConnectionTerminated()
        new_event.error_code = h2.errors._error_code_from_int(frame.error_code)
        new_event.last_stream_id = frame.last_stream_id
        
        if frame.additional_data:
            new_event.additional_data = frame.additional_data

        return [], [new_event]

    def _receive_naked_continuation(self, frame, stream: AsyncStream):
        return None, None

    def _receive_alt_svc_frame(self, frame, stream: AsyncStream):
        return None, None

    def _receive_unknown_frame(self, frame, stream: AsyncStream):
        return None, None

    async def _receive_events(self, stream: ReaderWriter) -> None:
        
        data = await stream.read()
        frame_buffer = self.frame_buffers[stream.stream_id]

        frame_buffer.add_data(data)
        frame_buffer.max_frame_size = stream.max_outbound_frame_size

        events = []

        for frame in frame_buffer:
            events.extend(self._receive_frame(frame, stream))

        stream.write(self._data_to_send)
        self._data_to_send = b''

        return events

    def _receive_frame(self, frame, stream: ReaderWriter):
        events = []
        try:
            frames, stream_events = self._frame_dispatch_table[frame.frame_type](frame, stream)
            
            if stream_events:
                events.extend(stream_events)

        except StreamResetException as rst:
            raise rst

        except StreamClosedError as e:
            # If the stream was closed by RST_STREAM, we just send a RST_STREAM
            # to the remote peer. Otherwise, this is a connection error, and so
            # we will re-raise to trigger one.
            if stream.closed_by in (StreamClosedBy.RECV_RST_STREAM, StreamClosedBy.SEND_RST_STREAM):
                f = RstStreamFrame(e.stream_id)
                f.error_code = e.error_code

                self._data_to_send += f.serialize()

                events = e._events
            else:
                raise Exception(f'Connection {stream.stream_id} err- {str(e)}')

        except Exception as e:
            raise Exception(f'Connection {stream.stream_id} err- {str(e)}')
                

        else:
            if frames:
                self._data_to_send += b''.join(f.serialize() for f in frames)
        
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

            self.outbound_flow_control_window = (
                self.remote_settings.initial_window_size
            )

        stream.inbound = WindowManager(self.local_settings.initial_window_size)
        stream.outbound = WindowManager(self.remote_settings.initial_window_size)
        stream.max_inbound_frame_size = self.local_settings.max_frame_size
        stream.max_outbound_frame_size = self.remote_settings.max_frame_size
        stream.current_outbound_window_size = self.remote_settings.initial_window_size


        self.frame_buffers[stream.stream_id] = FrameBuffer()

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
                    conn_manager = self._inbound_flow_control_window_manager
                    conn_increment = conn_manager.process_bytes(amount)
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

        # We only want to re-encode the headers if we're told to.
        if request.headers.hpack_encoder.header_table.maxsize != self._encoder.header_table.maxsize:
            request.headers.hpack_encoder.header_table_size = self._encoder.header_table.maxsize
            request.headers.setup_http2_headers(
                request.method,
                request.url
            )

        # # Slice into blocks of max_outbound_frame_size. Be careful with this:
        # # it only works right because we never send padded frames or priority
        # # information on the frames. Revisit this if we do.
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