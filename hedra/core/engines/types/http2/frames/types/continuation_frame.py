from typing import Any
from .attributes import Flag
from .utils import raw_data_repr
from .base_frame import Frame
from .attributes import _STREAM_ASSOC_HAS_STREAM


class ContinuationFrame(Frame):
    frame_type='CONTINUATION'
    """
    The CONTINUATION frame is used to continue a sequence of header block
    fragments. Any number of CONTINUATION frames can be sent on an existing
    stream, as long as the preceding frame on the same stream is one of
    HEADERS, PUSH_PROMISE or CONTINUATION without the END_HEADERS flag set.

    Much like the HEADERS frame, hyper treats this as an opaque data frame with
    different flags and a different type.
    """
    #: The flags defined for CONTINUATION frames.
    defined_flags = [Flag('END_HEADERS', 0x04)]

    #: The type byte defined for CONTINUATION frames.
    type = 0x09

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def __init__(self, stream_id: int, data: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The HPACK-encoded header block.
        self.data = data

    def _body_repr(self) -> str:
        return "data={}".format(
            raw_data_repr(self.data),
        )

    def serialize_body(self) -> bytes:
        return self.data

    def parse_body(self, data: bytearray) -> None:
        self.data = data
        self.body_len = len(data)