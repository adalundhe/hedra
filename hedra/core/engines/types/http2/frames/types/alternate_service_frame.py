from typing import Any
from .attributes import _STREAM_ASSOC_EITHER, _STRUCT_H
from .base_frame import Frame


class AltSvcFrame(Frame):
    frame_type = 'ALTSVC'
    """
    The ALTSVC frame is used to advertise alternate services that the current
    host, or a different one, can understand. This frame is standardised as
    part of RFC 7838.

    This frame does no work to validate that the ALTSVC field parameter is
    acceptable per the rules of RFC 7838.

    .. note:: If the ``stream_id`` of this frame is nonzero, the origin field
              must have zero length. Conversely, if the ``stream_id`` of this
              frame is zero, the origin field must have nonzero length. Put
              another way, a valid ALTSVC frame has ``stream_id != 0`` XOR
              ``len(origin) != 0``.
    """
    type = 0xA

    stream_association = _STREAM_ASSOC_EITHER

    def __init__(self, stream_id: int, origin: bytes = b'', field: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        self.origin = origin
        self.field = field

    def _body_repr(self) -> str:
        return "origin={!r}, field={!r}".format(
            self.origin,
            self.field,
        )

    def serialize_body(self) -> bytes:
        origin_len = _STRUCT_H.pack(len(self.origin))
        return origin_len + self.origin + self.field

    def parse_body(self, data: bytearray) -> None:
        origin_len = _STRUCT_H.unpack(data[0:2])[0]
        self.origin = data[2:2+origin_len]

        if len(self.origin) != origin_len:
            raise Exception("Invalid ALTSVC frame body.")

        self.field = data[2+origin_len:]

        self.body_len = len(data)