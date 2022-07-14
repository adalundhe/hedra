from typing import List, Any
from .base_frame import Frame
from .attributes import (
    Flag,
    _STREAM_ASSOC_EITHER,
    _STRUCT_L
)


class WindowUpdateFrame(Frame):
    frame_type='WINDOWUPDATE'
    """
    The WINDOW_UPDATE frame is used to implement flow control.

    Flow control operates at two levels: on each individual stream and on the
    entire connection.

    Both types of flow control are hop by hop; that is, only between the two
    endpoints. Intermediaries do not forward WINDOW_UPDATE frames between
    dependent connections. However, throttling of data transfer by any receiver
    can indirectly cause the propagation of flow control information toward the
    original sender.
    """
    #: The flags defined for WINDOW_UPDATE frames.
    defined_flags: List[Flag] = []

    #: The type byte defined for WINDOW_UPDATE frames.
    type = 0x08

    stream_association = _STREAM_ASSOC_EITHER

    def __init__(self, stream_id: int, window_increment: int = 0, **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The amount the flow control window is to be incremented.
        self.window_increment = window_increment

    def _body_repr(self) -> str:
        return "window_increment={}".format(
            self.window_increment,
        )

    def serialize_body(self) -> bytes:
        return _STRUCT_L.pack(self.window_increment & 0x7FFFFFFF)

    def parse_body(self, data: bytearray) -> None:
        if len(data) > 4:
            raise Exception(
                "WINDOW_UPDATE frame must have 4 byte length: got %s" %
                len(data)
            )

        self.window_increment = _STRUCT_L.unpack(data)[0]

        if not 1 <= self.window_increment <= 2**31-1:
            raise Exception(
                "WINDOW_UPDATE increment must be between 1 to 2^31-1"
            )

        self.body_len = 4