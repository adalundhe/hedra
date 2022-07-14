from typing import Any
from .attributes import (
    Flag,
    _STREAM_ASSOC_HAS_STREAM,
    Padding
)
from .base_frame import Frame


class DataFrame(Padding, Frame):
    frame_type='DATA'
    """
    DATA frames convey arbitrary, variable-length sequences of octets
    associated with a stream. One or more DATA frames are used, for instance,
    to carry HTTP request or response payloads.
    """
    #: The flags defined for DATA frames.
    defined_flags = [
        Flag('END_STREAM', 0x01),
        Flag('PADDED', 0x08),
    ]

    #: The type byte for data frames.
    type = 0x0

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def __init__(self, stream_id: int, data: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The data contained on this frame.
        self.data = data

    def serialize_body(self) -> bytes:
        padding_data = self.serialize_padding_data()
        padding = b'\0' * self.pad_length
        return padding_data + self.data + padding

    def parse_body(self, data: bytearray) -> None:
        padding_data_length = self.parse_padding_data(data)
        self.data = (
            data[padding_data_length:len(data)-self.pad_length]
        )
        self.body_len = len(data)

        if self.pad_length and self.pad_length >= self.body_len:
            raise Exception("Padding is too long.")

    @property
    def flow_controlled_length(self) -> int:
        """
        The length of the frame that needs to be accounted for when considering
        flow control.
        """
        padding_len = 0
        if 'PADDED' in self.flags:
            # Account for extra 1-byte padding length field, which is still
            # present if possibly zero-valued.
            padding_len = self.pad_length + 1
        return len(self.data) + padding_len