from typing import Any
from .base_frame import Frame
from .attributes import _STREAM_ASSOC_EITHER, _STRUCT_HBBBL
from .utils import raw_data_repr


class ExtensionFrame(Frame):
    __slots__ = (
        'type',
        'flag_byte',
        'body'
    )

    frame_type='EXTENSION'
    """
    ExtensionFrame is used to wrap frames which are not natively interpretable
    by hyperframe.

    Although certain byte prefixes are ordained by specification to have
    certain contextual meanings, frames with other prefixes are not prohibited,
    and may be used to communicate arbitrary meaning between HTTP/2 peers.

    Thus, hyperframe, rather than raising an exception when such a frame is
    encountered, wraps it in a generic frame to be properly acted upon by
    upstream consumers which might have additional context on how to use it.

    .. versionadded:: 5.0.0
    """

    stream_association = _STREAM_ASSOC_EITHER

    def __init__(self, type: int, stream_id: int, flag_byte: int = 0x0, body: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)
        self.type = type
        self.flag_byte = flag_byte
        self.body = body

    def _body_repr(self) -> str:
        return "type={}, flag_byte={}, body={}".format(
            self.type,
            self.flag_byte,
            raw_data_repr(self.body),
        )

    def parse_flags(self, flag_byte: int) -> None:  # type: ignore
        """
        For extension frames, we parse the flags by just storing a flag byte.
        """
        self.flag_byte = flag_byte

    def parse_body(self, data: bytearray) -> None:
        self.body = data
        self.body_len = len(data)

    def serialize(self) -> bytes:
        """
        A broad override of the serialize method that ensures that the data
        comes back out exactly as it came in. This should not be used in most
        user code: it exists only as a helper method if frames need to be
        reconstituted.
        """
        # Build the frame header.
        # First, get the flags.
        flags = self.flag_byte

        header = _STRUCT_HBBBL.pack(
            (self.body_len >> 8) & 0xFFFF,  # Length spread over top 24 bits
            self.body_len & 0xFF,
            self.type,
            flags,
            self.stream_id & 0x7FFFFFFF  # Stream ID is 32 bits.
        )

        return header + self.body