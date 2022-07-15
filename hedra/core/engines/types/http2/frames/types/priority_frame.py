from typing import List
from .base_frame import Frame
from .attributes import (
    Priority,
    Flag,
    _STREAM_ASSOC_HAS_STREAM
)


class PriorityFrame(Priority, Frame):
    frame_type='PRIORITY'
    """
    The PRIORITY frame specifies the sender-advised priority of a stream. It
    can be sent at any time for an existing stream. This enables
    reprioritisation of existing streams.
    """
    #: The flags defined for PRIORITY frames.
    defined_flags: List[Flag] = []

    #: The type byte defined for PRIORITY frames.
    type = 0x02

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def _body_repr(self) -> str:
        return "exclusive={}, depends_on={}, stream_weight={}".format(
            self.exclusive,
            self.depends_on,
            self.stream_weight
        )

    def serialize_body(self) -> bytes:
        return self.serialize_priority_data()

    def parse_body(self, data: bytearray) -> None:
        self.parse_priority_data(data)
        self.body_len = 5