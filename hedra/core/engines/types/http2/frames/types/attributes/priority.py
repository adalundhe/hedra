import struct
from typing import Any
from .struct_types import _STRUCT_LB


class Priority:

    """
    Mixin for frames that contain priority data. Defines extra fields that can
    be used and set by frames that contain priority data.
    """
    def __init__(self,
                 stream_id: int,
                 depends_on: int = 0x0,
                 stream_weight: int = 0x0,
                 exclusive: bool = False,
                 **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)  # type: ignore

        #: The stream ID of the stream on which this stream depends.
        self.depends_on = depends_on

        #: The weight of the stream. This is an integer between 0 and 256.
        self.stream_weight = stream_weight

        #: Whether the exclusive bit was set.
        self.exclusive = exclusive

    def serialize_priority_data(self) -> bytes:
        return _STRUCT_LB.pack(
            self.depends_on + (0x80000000 if self.exclusive else 0),
            self.stream_weight
        )

    def parse_priority_data(self, data: bytearray) -> int:
        try:
            self.depends_on, self.stream_weight = _STRUCT_LB.unpack(data[:5])
        except struct.error:
            raise Exception("Invalid Priority data")

        self.exclusive = True if self.depends_on >> 31 else False
        self.depends_on &= 0x7FFFFFFF
        return 5
