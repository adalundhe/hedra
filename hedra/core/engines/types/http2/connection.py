from __future__ import annotations
import asyncio
import h2.stream
import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
import h2.errors
from asyncio import events
from h2.windows import WindowManager
from hpack import Encoder, Decoder
from hyperframe.frame import SettingsFrame
from h2.frame_buffer import FrameBuffer
from enum import IntEnum
from .stream import AsyncStream
from hyperframe.frame import (
    GoAwayFrame, WindowUpdateFrame, HeadersFrame, DataFrame, PingFrame,
    SettingsFrame, RstStreamFrame, PriorityFrame,
    ContinuationFrame, AltSvcFrame, ExtensionFrame
)
from hedra.core.engines.types.common import Response, Request



class StreamReset(Exception):
    pass


class HTTPConnectionState(IntEnum):
    ACTIVE = 1
    IDLE = 2
    CLOSED = 3


class HTTP2Connection:
    CONFIG = h2.config.H2Configuration(validate_inbound_headers=False)

    def __init__(self, pool_id):
        self.pool_id = pool_id
        self._network_stream = None
        self._h2_state = h2.connection.H2Connection(config=self.CONFIG)
        self._state = HTTPConnectionState.IDLE
        self.connected = False
        self._encoder = Encoder()
        self._decoder = Decoder()
        self._init_sent = False
        self.stream_id = None
        self._data_to_send = b''
        self.streams = {}
        self.frame_buffers = {}
        self._headers_sent = False
        self.lock = asyncio.Lock()

        self.local_settings = h2.settings.Settings(
            client=True,
            initial_values={
                h2.settings.SettingCodes.ENABLE_PUSH: 0,
                h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS: 1000,
                h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE: 65535,
            }
        )
        self.remote_settings = h2.settings.Settings(
            client=False
        )

        self.outbound_flow_control_window = (
            self.remote_settings.initial_window_size
        )

        del self.local_settings[h2.settings.SettingCodes.ENABLE_CONNECT_PROTOCOL]

        self._inbound_flow_control_window_manager = WindowManager(
            max_window_size=self.local_settings.initial_window_size
        )

        self._frame_dispatch_table = {
            HeadersFrame: self._receive_headers_frame,
            SettingsFrame: self._receive_settings_frame,
            DataFrame: self._receive_data_frame,
            WindowUpdateFrame: self._receive_window_update_frame,
            PingFrame: self._receive_ping_frame,
            RstStreamFrame: self._receive_rst_stream_frame,
            PriorityFrame: self._receive_priority_frame,
            GoAwayFrame: self._receive_goaway_frame,
            ContinuationFrame: self._receive_naked_continuation,
            AltSvcFrame: self._receive_alt_svc_frame,
            ExtensionFrame: self._receive_unknown_frame
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

    def _receive_headers_frame(self, frame, stream: AsyncStream):
        headers =  self._decoder.decode(frame.data, raw=True)
        frames, stream_events = self.streams[stream.stream_id].receive_headers(
            headers,
            'END_STREAM' in frame.flags,
            self._h2_state.config.header_encoding
        )

        if 'PRIORITY' in frame.flags:
            p_frames, p_events = self._receive_priority_frame(frame)
            stream_events[0].priority_updated = p_events[0]
            stream_events.extend(p_events)
            assert not p_frames

        return frames, stream_events

    def _receive_settings_frame(self, frame, stream: AsyncStream):
        events = []
        if 'ACK' in frame.flags:

            changes = self.local_settings.acknowledge()
            if h2.settings.SettingCodes.INITIAL_WINDOW_SIZE in changes:
                setting = changes[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE]
                self.streams[stream.stream_id]._inbound_flow_control_change_from_settings(
                    setting.new_value - setting.original_value
                )

            if h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE in changes:
                setting = changes[h2.settings.SettingCodes.MAX_HEADER_LIST_SIZE]
                self._decoder.max_header_list_size = setting.new_value

            if h2.settings.SettingCodes.MAX_FRAME_SIZE in changes:
                setting = changes[h2.settings.SettingCodes.MAX_FRAME_SIZE]
                self.streams[stream.stream_id].max_outbound_frame_size = setting.new_value

            if h2.settings.SettingCodes.HEADER_TABLE_SIZE in changes:
                setting = changes[h2.settings.SettingCodes.HEADER_TABLE_SIZE]
                # This is safe across all hpack versions: some versions just won't
                # respect it.
                self._decoder.max_allowed_table_size = setting.new_value

            ack_event = h2.events.SettingsAcknowledged()
            ack_event.changed_settings = changes
            events.append(ack_event)
            return [], events

        # Add the new settings.
        self.remote_settings.update(frame.settings)
        events.append(
            h2.events.RemoteSettingsChanged.from_settings(
                self.remote_settings, frame.settings
            )
        )

        changes = self.remote_settings.acknowledge()
     
        if h2.settings.SettingCodes.INITIAL_WINDOW_SIZE in changes:
            setting = changes[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE]
            self.streams[stream.stream_id].outbound_flow_control_window = self._guard_increment_window(
                self.streams[stream.stream_id].outbound_flow_control_window,
                setting.new_value - setting.original_value
            )

        # HEADER_TABLE_SIZE changes by the remote part affect our encoder: cf.
        # RFC 7540 Section 6.5.2.
        if  h2.settings.SettingCodes.HEADER_TABLE_SIZE in changes:
            setting = changes[ h2.settings.SettingCodes.HEADER_TABLE_SIZE]
            self._encoder.header_table_size = setting.new_value

        if  h2.settings.SettingCodes.MAX_FRAME_SIZE in changes:
            setting = changes[ h2.settings.SettingCodes.MAX_FRAME_SIZE]
            self.streams[stream.stream_id].max_outbound_frame_size = setting.new_value

        frames = SettingsFrame(0)
        frames.flags.add('ACK')

        return [frames], events

    def _receive_data_frame(self, frame, stream: AsyncStream):
        flow_controlled_length = frame.flow_controlled_length
        frames = []
        events = []
        self._inbound_flow_control_window_manager.window_consumed(
            flow_controlled_length
        )


        try:
            frames, events = self.streams[stream.stream_id].receive_data(
                frame.data,
                'END_STREAM' in frame.flags,
                flow_controlled_length
            )
        except h2.exceptions.StreamClosedError as e:
            # This stream is either marked as CLOSED or already gone from our
            # internal state.
            
            conn_manager = self._inbound_flow_control_window_manager
            conn_increment = conn_manager.process_bytes(
                frame.flow_controlled_length
            )

            if conn_increment:
                f = WindowUpdateFrame(0)
                f.window_increment = conn_increment
                frames.append(f)

            f = RstStreamFrame(e.stream_id)
            f.error_code = e.error_code
            frames.append(f)
    
            return frames, events + e._events

        return frames, events       
    
    def _receive_window_update_frame(self, frame, stream: AsyncStream):
        events = []
        if frame.stream_id:
            try:
                frames, stream_events = self.streams[stream.stream_id].receive_window_update(
                    frame.window_increment
                )

                events.extend(stream_events)
            except Exception as e:
                return [], events
        else:
            self.outbound_flow_control_window = self._guard_increment_window(
                self.outbound_flow_control_window,
                frame.window_increment
            )
            # FIXME: Should we split this into one event per active stream?
            window_updated_event = h2.events.WindowUpdated()
            window_updated_event.stream_id = 0
            window_updated_event.delta = frame.window_increment
            events.append(window_updated_event)
            frames = []

        return frames, events

    def _receive_ping_frame(self, frame, stream: AsyncStream):
        return None, None

    def _receive_rst_stream_frame(self, frame, stream: AsyncStream):
        stream_frames, stream_events = self.streams[stream.stream_id].stream_reset(frame)
        return stream_frames, events + stream_events

    def _receive_priority_frame(self, frame, stream: AsyncStream):
        return None, None

    def _receive_goaway_frame(self, frame, stream: AsyncStream):
        self._data_to_send = b''

        new_event = h2.events.ConnectionTerminated()
        new_event.error_code = h2.errors._error_code_from_int(frame.error_code)
        new_event.last_stream_id = frame.last_stream_id
        new_event.additional_data = (frame.additional_data
                                     if frame.additional_data else None)

        return [], [new_event]

    def _receive_naked_continuation(self, frame, stream: AsyncStream):
        return None, None

    def _receive_alt_svc_frame(self, frame, stream: AsyncStream):
        return None, None

    def _receive_unknown_frame(self, frame, stream: AsyncStream):
        return None, None

    async def _receive_events(self, stream: AsyncStream) -> None:
        data = await self._network_stream.read()
        self.frame_buffers[stream.stream_id].add_data(data)
        self.frame_buffers[stream.stream_id].max_frame_size = self.streams[stream.stream_id].max_outbound_frame_size

        events = []

        for frame in self.frame_buffers[stream.stream_id]:
            events.extend(self._receive_frame(frame, stream))

        self._network_stream.write(self._data_to_send)
        self._data_to_send = b''

        return events

    def _receive_frame(self, frame, stream: AsyncStream):
        events = []
        try:
            # I don't love using __class__ here, maybe reconsider it.
            frames, stream_events = self._frame_dispatch_table[frame.__class__](frame, stream)
            if stream_events is None:
                events.append(stream_events)
            
            elif stream_events == b'':
                pass

            else:
                events.extend(stream_events)

        except StreamReset as rst:
            raise rst

        except h2.exceptions.StreamClosedError as e:
            # If the stream was closed by RST_STREAM, we just send a RST_STREAM
            # to the remote peer. Otherwise, this is a connection error, and so
            # we will re-raise to trigger one.
            if self.streams[stream.stream_id].closed_by in (h2.stream.StreamClosedBy.RECV_RST_STREAM, h2.stream.StreamClosedBy.SEND_RST_STREAM):
                f = RstStreamFrame(e.stream_id)
                f.error_code = e.error_code

                self._data_to_send += f.serialize()

                events = e._events
            else:
                raise Exception(f'Connection {self.pool_id} err- {str(e)}')

        except Exception as e:
            raise Exception(f'Connection {self.pool_id} err- {str(e)}')
                

        else:
            if frames:
                self._data_to_send += b''.join(f.serialize() for f in frames)

        return events
    
    def connect(self, stream: AsyncStream):
        
        self._network_stream = stream

        self._headers_sent = False

        if self._init_sent is False or self._network_stream.reset_connection:
            self._data_to_send = b'PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n'

            f = SettingsFrame(0)
            for setting, value in self.local_settings.items():
                f.settings[setting] = value

            self._data_to_send += f.serialize()

            self._inbound_flow_control_window_manager.window_opened(65536)
            f = WindowUpdateFrame(0)
            f.window_increment = 65536
            self._data_to_send += f.serialize()

            self._network_stream.write(self._data_to_send)
            self._data_to_send = b''
            self._init_sent = True

            self.outbound_flow_control_window = (
                self.remote_settings.initial_window_size
            )

        self.streams[stream.stream_id] = h2.connection.H2Stream(
            stream.stream_id, 
            self.CONFIG,
            inbound_window_size=self.local_settings.initial_window_size,
            outbound_window_size=self.remote_settings.initial_window_size
        )

        self.frame_buffers[stream.stream_id] = FrameBuffer(server=False)

        self.streams[stream.stream_id].max_inbound_frame_size = self.local_settings.max_frame_size
        self.streams[stream.stream_id].max_outbound_frame_size = self.remote_settings.max_frame_size

    async def receive_response(self, response: Response, stream: AsyncStream):
       
        done = False
        while done is False:
            events = await self._receive_events(stream)
            for event in events:
                if hasattr(event, "error_code"):
                    raise Exception(f'Connection - {self.pool_id} err: {str(event)}')

                elif isinstance(event, h2.events.ResponseReceived):
                    for k, v in event.headers:
                        if k == b":status":
                            status_code = int(v.decode("ascii", errors="ignore"))
                            response.response_code = status_code
                        elif not k.startswith(b":"):
                            response.headers[k] = v

                elif isinstance(event, h2.events.DataReceived):
                    amount = event.flow_controlled_length
                    frames = []
                    conn_manager = self._inbound_flow_control_window_manager
                    conn_increment = conn_manager.process_bytes(amount)
                    if conn_increment:
                        f = WindowUpdateFrame(0)
                        f.window_increment = conn_increment
                        frames.append(f)

                    frames += self.streams[stream.stream_id].acknowledge_received_data(amount)
                    self._data_to_send += b''.join([frame for frame in frames])
                    self._network_stream.write(self._data_to_send)
                    self._data_to_send = b''

                    response.body += event.data

                elif isinstance(event, (h2.events.StreamEnded, h2.events.StreamReset)):
                    done = True
                    break

                elif event == b'' or event is None:
                    raise Exception('Unexpected Event!')

        return response

    def send_request_headers(self, request: Request, stream: AsyncStream) -> None:
        end_stream = request.payload.has_data is False
        frames = self.streams[stream.stream_id].send_headers(request.headers.encoded_headers, self._encoder, end_stream=end_stream)
    
        headers = b''.join(frame.serialize() for frame in frames)

        frames = self.streams[stream.stream_id].increase_flow_control_window(65536)
        buffer = b''.join([frame.serialize() for frame in frames])

        self._network_stream.write(headers + buffer)
        self._data_to_send = b''

        self._headers_sent = True

    async def submit_request_body(self, request: Request, stream: AsyncStream) -> None:
        if request.payload.has_data:
            data = request.payload.encoded_data
            
            while data:
                local_flow = self.streams[stream.stream_id].outbound_flow_control_window
                max_frame_size = self.streams[stream.stream_id].max_outbound_frame_size
                flow = min(local_flow, max_frame_size)
                while flow == 0:
                    await self._receive_events(stream)
                    local_flow = self.streams[stream.stream_id].outbound_flow_control_window
                    max_frame_size = self.streams[stream.stream_id].max_outbound_frame_size
                    flow = min(local_flow, max_frame_size)
                    
                max_flow = flow
                chunk_size = min(len(data), max_flow)
                chunk, data = data[:chunk_size], data[chunk_size:]
                frames = self.streams[stream.stream_id].send_data(chunk)
                self._network_stream.write(b''.join([frame.serialize() for frame in frames]))
                self._data_to_send = b''


            frames = self.streams[stream.stream_id].end_stream()
            self._network_stream.write(b''.join([frame.serialize() for frame in frames]))
            self._data_to_send = b''
   