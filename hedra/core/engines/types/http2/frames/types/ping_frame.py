from typing import Any
from .base_frame import Frame
from .attributes import (
    _STREAM_ASSOC_NO_STREAM,
    Flag
)


class PingFrame(Frame):
    frame_type='PING'
    """
    The PING frame is a mechanism for measuring a minimal round-trip time from
    the sender, as well as determining whether an idle connection is still
    functional. PING frames can be sent from any endpoint.
    """
    #: The flags defined for PING frames.
    defined_flags = [Flag('ACK', 0x01)]

    #: The type byte defined for PING frames.
    type = 0x06

    stream_association = _STREAM_ASSOC_NO_STREAM

    def __init__(self, stream_id: int = 0, opaque_data: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The opaque data sent in this PING frame, as a bytestring.
        self.opaque_data = opaque_data

    def _body_repr(self) -> str:
        return "opaque_data={!r}".format(
            self.opaque_data,
        )

    def serialize_body(self) -> bytes:
        data = self.opaque_data
        data += b'\x00' * (8 - len(self.opaque_data))
        return data

    def parse_body(self, data: bytearray) -> None:
        self.opaque_data = data
        self.body_len = 8

