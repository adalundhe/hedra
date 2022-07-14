from typing import List, Any
from .base_frame import Frame
from .attributes import (
    Flag,
    _STREAM_ASSOC_NO_STREAM,
    _STRUCT_LL
)


class GoAwayFrame(Frame):
    frame_type='GOAWAY'
    """
    The GOAWAY frame informs the remote peer to stop creating streams on this
    connection. It can be sent from the client or the server. Once sent, the
    sender will ignore frames sent on new streams for the remainder of the
    connection.
    """
    #: The flags defined for GOAWAY frames.
    defined_flags: List[Flag] = []

    #: The type byte defined for GOAWAY frames.
    type = 0x07

    stream_association = _STREAM_ASSOC_NO_STREAM

    def __init__(self,
                 stream_id: int = 0,
                 last_stream_id: int = 0,
                 error_code: int = 0,
                 additional_data: bytes = b'',
                 **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The last stream ID definitely seen by the remote peer.
        self.last_stream_id = last_stream_id

        #: The error code for connection teardown.
        self.error_code = error_code

        #: Any additional data sent in the GOAWAY.
        self.additional_data = additional_data

    def _body_repr(self) -> str:
        return "last_stream_id={}, error_code={}, additional_data={!r}".format(
            self.last_stream_id,
            self.error_code,
            self.additional_data,
        )

    def serialize_body(self) -> bytes:
        data = _STRUCT_LL.pack(
            self.last_stream_id & 0x7FFFFFFF,
            self.error_code
        )
        data += self.additional_data

        return data

    def parse_body(self, data: bytearray) -> None:
        self.last_stream_id, self.error_code = _STRUCT_LL.unpack(data[:8])

        self.body_len = len(data)

        if len(data) > 8:
            self.additional_data = data[8:]
