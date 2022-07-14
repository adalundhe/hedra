import struct
from typing import Any
from .struct_types import _STRUCT_B


class Padding:
    """
    Mixin for frames that contain padding. Defines extra fields that can be
    used and set by frames that can be padded.
    """
    def __init__(self, stream_id: int, pad_length: int = 0, **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)  # type: ignore

        #: The length of the padding to use.
        self.pad_length = pad_length

    def serialize_padding_data(self) -> bytes:
        if 'PADDED' in self.flags:  # type: ignore
            return _STRUCT_B.pack(self.pad_length)
        return b''

    def parse_padding_data(self, data: bytearray) -> int:
        if 'PADDED' in self.flags:  # type: ignore
            try:
                self.pad_length = struct.unpack('!B', data[:1])[0]
            except struct.error:
                raise Exception("Invalid Padding data")
            return 1
        return 0

    #: .. deprecated:: 5.2.1
    #:    Use self.pad_length instead.
    @property
    def total_padding(self) -> int:  # pragma: no cover
        import warnings
        warnings.warn(
            "total_padding contains the same information as pad_length.",
            DeprecationWarning
        )
        return self.pad_length