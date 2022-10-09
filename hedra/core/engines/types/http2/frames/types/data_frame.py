import struct
from typing import Any
from hedra.core.engines.types.http2.errors.exceptions import StreamClosedError
from hedra.core.engines.types.http2.events.data_received_event import DataReceived
from hedra.core.engines.types.http2.events.stream_ended_event import StreamEnded
from hedra.core.engines.types.http2.frames.types.reset_stream_frame import RstStreamFrame
from hedra.core.engines.types.http2.stream import Stream
from .attributes import (
    Flag,
    _STREAM_ASSOC_HAS_STREAM,
    Padding
)
from .base_frame import Frame


class DataFrame(Frame):
    __slots__ = (
        'data',
        'pad_length'
    )

    frame_type='DATA'
    """
    DATA frames convey arbitrary, variable-length sequences of octets
    associated with a stream. One or more DATA frames are used, for instance,
    to carry HTTP request or response payloads.
    """
    #: The flags defined for DATA frames.
    defined_flags = [
        Flag('END_STREAM', 0x01),
        Flag('PADDED', 0x08),
    ]

    #: The type byte for data frames.
    type = 0x0

    stream_association = _STREAM_ASSOC_HAS_STREAM

    def __init__(self, stream_id: int, data: bytes = b'', **kwargs: Any) -> None:
        super().__init__(stream_id, **kwargs)

        #: The data contained on this frame.
        self.data = data
        self.pad_length = kwargs.get('pad_length', 0)

    def serialize_body(self) -> bytes:
        padding_data = self.serialize_padding_data()
        padding = b'\0' * self.pad_length
        return padding_data + self.data + padding

    def parse_body(self, data: bytearray) -> None:

        padding_data_length = 0

        if 'PADDED' in self.flags:  # type: ignore
            self.pad_length = struct.unpack('!B', data[:1])[0]
            padding_data_length = 1

        data_length = len(data)
        self.data = (
            data[padding_data_length:data_length-self.pad_length]
        )
        self.body_len = data_length

    @property
    def flow_controlled_length(self) -> int:
        """
        The length of the frame that needs to be accounted for when considering
        flow control.
        """
        padding_len = 0
        if 'PADDED' in self.flags:
            # Account for extra 1-byte padding length field, which is still
            # present if possibly zero-valued.
            padding_len = self.pad_length + 1
        return len(self.data) + padding_len

    def get_events_and_frames(self, stream: Stream, connection):
        end_stream = 'END_STREAM' in self.flags
        flow_controlled_length = self.flow_controlled_length
        frame_data = self.data

        frames = []
        data_events = []
        connection._inbound_flow_control_window_manager.window_consumed(
            flow_controlled_length
        )

        try:
            
            stream.inbound.window_consumed(flow_controlled_length)
      
            event = DataReceived()
            event.stream_id = stream.stream_id

            data_events.append(event)

            if end_stream:
                event = StreamEnded()
                event.stream_id = stream.stream_id
                data_events[0].stream_ended = event
                data_events.append(event)

            data_events[0].data = frame_data
            data_events[0].flow_controlled_length = flow_controlled_length
            return frames, data_events

        except StreamClosedError as e:
            # This stream is either marked as CLOSED or already gone from our
            # internal state.
            
            conn_manager = connection._inbound_flow_control_window_manager
            conn_increment = conn_manager.process_bytes(
                flow_controlled_length
            )

            if conn_increment:
                f = Frame(0)
                f.window_increment = conn_increment
                frames.append(f)

            f = RstStreamFrame(e.stream_id)
            f.error_code = e.error_code
            frames.append(f)
    
            return frames, data_events + e._events