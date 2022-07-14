import struct
from typing import List, Any
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
        return "error_code={}".format(
            self.error_code,
        )

    def serialize_body(self) -> bytes:
        return _STRUCT_L.pack(self.error_code)

    def parse_body(self, data: bytearray) -> None:
        if len(data) != 4:
            raise Exception(
                "RST_STREAM must have 4 byte body: actual length %s." %
                len(data)
            )

        try:
            self.error_code = _STRUCT_L.unpack(data)[0]
        except struct.error:  # pragma: no cover
            raise Exception("Invalid RST_STREAM body")

        self.body_len = 4