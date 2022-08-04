from typing import List, Any
from hedra.core.engines.types.http2.errors.exceptions import StreamError
from hedra.core.engines.types.http2.errors.types import ErrorCodes
from hedra.core.engines.types.http2.events.stream_reset import StreamReset
from hedra.core.engines.types.http2.events.window_updated_event import WindowUpdated
from hedra.core.engines.types.http2.frames.types.reset_stream_frame import RstStreamFrame
from hedra.core.engines.types.http2.reader_writer import ReaderWriter
from .base_frame import Frame
from .attributes import (
    Flag,
    _STREAM_ASSOC_EITHER,
    _STRUCT_L
)


class WindowUpdateFrame(Frame):
    frame_type='WINDOWUPDATE'
    """
    The WINDOW_UPDATE frame is used to implement flow control.

    Flow control operates at two levels: on each individual stream and on the
    entire connection.

    Both types of flow control are hop by hop; that is, only between the two
    endpoints. Intermediaries do not forward WINDOW_UPDATE frames between
    dependent connections. However, throttling of data transfer by any receiver
    can indirectly cause the propagation of flow control information toward the
    original sender.
    """
    #: The flags defined for WINDOW_UPDATE frames.
    defined_flags: List[Flag] = []

    #: The type byte defined for WINDOW_UPDATE frames.
    type = 0x08

    stream_association = _STREAM_ASSOC_EITHER

    def __init__(self, stream_id: int, window_increment: int = 0, **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The amount the flow control window is to be incremented.
        self.window_increment = window_increment

    def _body_repr(self) -> str:
        return "window_increment={}".format(
            self.window_increment,
        )

    def serialize_body(self) -> bytes:
        return _STRUCT_L.pack(self.window_increment & 0x7FFFFFFF)

    def parse_body(self, data: bytearray) -> None:
        self.window_increment = _STRUCT_L.unpack(data)[0]
        self.body_len = 4

    def get_events_and_frames(self, stream: ReaderWriter, connection):
        stream_events = []
        frames = []
        increment = self.window_increment
        if self.stream_id:
            try:

                
                event = WindowUpdated()
                event.stream_id = stream.stream_id

                # If we encounter a problem with incrementing the flow control window,
                # this should be treated as a *stream* error, not a *connection* error.
                # That means we need to catch the error and forcibly close the stream.
                event.delta = increment

                try:
                    connection.outbound_flow_control_window = connection._guard_increment_window(
                        connection.outbound_flow_control_window,
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
            except Exception:
                return [], stream_events
        else:
            connection.outbound_flow_control_window = connection._guard_increment_window(
                connection.outbound_flow_control_window,
                increment
            )
            # FIXME: Should we split this into one event per active stream?
            window_updated_event = WindowUpdated()
            window_updated_event.stream_id = 0
            window_updated_event.delta = increment
            stream_events.append(window_updated_event)
            frames = []

        return frames, stream_events