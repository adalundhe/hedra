from typing import Any
from hedra.core.engines.types.http2.events.deferred_headers_event import DeferredHeaders
from hedra.core.engines.types.http2.events.stream_ended_event import StreamEnded
from hedra.core.engines.types.http2.reader_writer import ReaderWriter
from .base_frame import Frame
from .attributes import (
    Padding,
    Priority,
    Flag,
    _STREAM_ASSOC_HAS_STREAM
)
from .utils import raw_data_repr


class HeadersFrame(Padding, Priority, Frame):
    frame_type='HEADERS'
    """
    The HEADERS frame carries name-value pairs. It is used to open a stream.
    HEADERS frames can be sent on a stream in the "open" or "half closed
    (remote)" states.

    The HeadersFrame class is actually basically a data frame in this
    implementation, because of the requirement to control the sizes of frames.
    A header block fragment that doesn't fit in an entire HEADERS frame needs
    to be followed with CONTINUATION frames. From the perspective of the frame
    building code the header block is an opaque data segment.
    """
    #: The flags defined for HEADERS frames.
    defined_flags = [
        Flag('END_STREAM', 0x01),
        Flag('END_HEADERS', 0x04),
        Flag('PADDED', 0x08),
        Flag('PRIORITY', 0x20),
    ]

    #: The type byte defined for HEADERS frames.
    type = 0x01

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def __init__(self, stream_id: int, data: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The HPACK-encoded header block.
        self.data = data

    def _body_repr(self) -> str:
        return "exclusive={}, depends_on={}, stream_weight={}, data={}".format(
            self.exclusive,
            self.depends_on,
            self.stream_weight,
            raw_data_repr(self.data),
        )

    def serialize_body(self) -> bytes:
        padding_data = self.serialize_padding_data()
        padding = b'\0' * self.pad_length

        if 'PRIORITY' in self.flags:
            priority_data = self.serialize_priority_data()
        else:
            priority_data = b''

        return padding_data + priority_data + self.data + padding

    def parse_body(self, data: bytearray) -> None:
        padding_data_length = self.parse_padding_data(data)
        data = data[padding_data_length:]

        if 'PRIORITY' in self.flags:
            priority_data_length = self.parse_priority_data(data)
        else:
            priority_data_length = 0

        self.body_len = len(data)
        self.data = data[priority_data_length:len(data)-self.pad_length]

    def get_events_and_frames(self, stream: ReaderWriter, connection):
        
        # Hyper H2 would have you immediate unpack the headers,
        # but as with the Encoder, the Decoder blocks the event loop.
        # Instead, since we know the HeadersFrame is valid, we'll defer
        # parsing the headers until explicitly needed (during results
        # analysis/accumulation).

        stream_events = []

        deferred_headers = DeferredHeaders(
            connection._decoder.header_table.maxsize,
            self,
            connection._h2_state.config.header_encoding
        )

        stream_events.append(deferred_headers)

        if deferred_headers.end_stream:
            event = StreamEnded()
            event.stream_id = stream.stream_id
            
            stream_events[0].stream_ended = event
            stream_events.append(event)

        return [], stream_events