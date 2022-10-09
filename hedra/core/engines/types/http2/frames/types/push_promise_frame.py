from typing import Any
from .attributes import (
    Padding,
    Flag,
    _STREAM_ASSOC_HAS_STREAM,
    _STRUCT_L
)
from .base_frame import Frame
from .utils import raw_data_repr



class PushPromiseFrame(Padding, Frame):
    __slots__ = (
        'promised_stream_id',
        'data'
    )

    frame_type='PUSHPROMISE'
    """
    The PUSH_PROMISE frame is used to notify the peer endpoint in advance of
    streams the sender intends to initiate.
    """
    #: The flags defined for PUSH_PROMISE frames.
    defined_flags = [
        Flag('END_HEADERS', 0x04),
        Flag('PADDED', 0x08)
    ]

    #: The type byte defined for PUSH_PROMISE frames.
    type = 0x05

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def __init__(self, stream_id: int, promised_stream_id: int = 0, data: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The stream ID that is promised by this frame.
        self.promised_stream_id = promised_stream_id

        #: The HPACK-encoded header block for the simulated request on the new
        #: stream.
        self.data = data

    def _body_repr(self) -> str:
        return "promised_stream_id={}, data={}".format(
            self.promised_stream_id,
            raw_data_repr(self.data),
        )

    def serialize_body(self) -> bytes:
        padding_data = self.serialize_padding_data()
        padding = b'\0' * self.pad_length
        data = _STRUCT_L.pack(self.promised_stream_id)
        return b''.join([padding_data, data, self.data, padding])

    def parse_body(self, data: bytearray) -> None:
        padding_data_length = self.parse_padding_data(data)

        self.promised_stream_id = _STRUCT_L.unpack(data[padding_data_length:padding_data_length + 4])[0]

        self.data = data[padding_data_length + 4:len(data)-self.pad_length]
        self.body_len = len(data)
