import struct
import h2.errors
from typing import List, Any
from hedra.core.engines.types.http2.events.stream_reset import StreamReset
from hedra.core.engines.types.http2.stream import Stream
from hedra.core.engines.types.http2.streams.stream_closed_by import StreamClosedBy
from .base_frame import Frame
from .attributes import (
    Flag,
    _STREAM_ASSOC_HAS_STREAM,
    _STRUCT_L
)


class RstStreamFrame(Frame):
    frame_type='RESET'
    """
    The RST_STREAM frame allows for abnormal termination of a stream. When sent
    by the initiator of a stream, it indicates that they wish to cancel the
    stream or that an error condition has occurred. When sent by the receiver
    of a stream, it indicates that either the receiver is rejecting the stream,
    requesting that the stream be cancelled or that an error condition has
    occurred.
    """
    #: The flags defined for RST_STREAM frames.
    defined_flags: List[Flag] = []

    #: The type byte defined for RST_STREAM frames.
    type = 0x03

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def __init__(self, stream_id: int, error_code: int = 0, **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The error code used when resetting the stream.
        self.error_code = error_code

    def _body_repr(self) -> str:
        return f"error_code={self.error_code}"

    def serialize_body(self) -> bytes:
        return _STRUCT_L.pack(self.error_code)

    def parse_body(self, data: bytearray) -> None:
        self.error_code = _STRUCT_L.unpack(data)[0]
        self.body_len = 4
    
    def get_events_and_frames(self, stream: Stream, connection):

        stream.closed_by = StreamClosedBy.RECV_RST_STREAM
        reset_event = StreamReset()
        reset_event.stream_id = stream.stream_id
        reset_event[0].error_code = h2.errors._error_code_from_int(self.error_code)

        return [], [reset_event]