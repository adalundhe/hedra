# -*- coding: utf-8 -*-
"""
hyperframe/frame
~~~~~~~~~~~~~~~~

Defines framing logic for HTTP/2. Provides both classes to represent framed
data and logic for aiding the connection when it comes to reading from the
socket.
"""

from typing import Optional, Tuple, List, Iterable

from .attributes import (
    Flag,
    Flags,
    _STREAM_ASSOC_HAS_STREAM,
    _STREAM_ASSOC_NO_STREAM,
    _STRUCT_HBBBL
)
from .utils import raw_data_repr




class Frame:
    FRAMES = {}
    """
    The base class for all HTTP/2 frames.
    """
    #: The flags defined on this type of frame.
    defined_flags: List[Flag] = []

    #: The byte used to define the type of the frame.
    type: Optional[int] = None

    # If 'has-stream', the frame's stream_id must be non-zero. If 'no-stream',
    # it must be zero. If 'either', it's not checked.
    stream_association: Optional[str] = None

    def __init__(self, stream_id: int, flags: Iterable[str] = ()) -> None:
        #: The stream identifier for the stream this frame was received on.
        #: Set to 0 for frames sent on the connection (stream-id 0).
        self.stream_id = stream_id

        #: The flags set for this frame.
        self.flags = Flags(self.defined_flags)

        #: The frame length, excluding the nine-byte header.
        self.body_len = 0

        for flag in flags:
            self.flags.add(flag)

        if (not self.stream_id and
           self.stream_association == _STREAM_ASSOC_HAS_STREAM):
            raise Exception(
                'Stream ID must be non-zero for {}'.format(
                    type(self).__name__,
                )
            )
        if (self.stream_id and
           self.stream_association == _STREAM_ASSOC_NO_STREAM):
            raise Exception(
                'Stream ID must be zero for {} with stream_id={}'.format(
                    type(self).__name__,
                    self.stream_id,
                )
            )

    def __repr__(self) -> str:
        body_repr = self._body_repr(),
        return f"{type(self).__name__}(stream_id={self.stream_id}, flags={repr(self.flags)}): {body_repr}"

    def _body_repr(self) -> str:
        # More specific implementation may be provided by subclasses of Frame.
        # This fallback shows the serialized (and truncated) body content.
        return raw_data_repr(self.serialize_body())

    @staticmethod
    def explain(data: memoryview) -> Tuple["Frame", int]:
        """
        Takes a bytestring and tries to parse a single frame and print it.

        This function is only provided for debugging purposes.

        :param data: A memoryview object containing the raw data of at least
                     one complete frame (header and body).

        .. versionadded:: 6.0.0
        """
        frame, length = Frame.parse_frame_header(data[:9])
        frame.parse_body(data[9:9 + length])
        print(frame)
        return frame, length

    def parse_flags(self, flag_byte: int) -> Flags:
        for flag, flag_bit in self.defined_flags:
            if flag_byte & flag_bit:
                self.flags.add(flag)

        return self.flags

    def serialize(self) -> bytes:
        """
        Convert a frame into a bytestring, representing the serialized form of
        the frame.
        """
        body = self.serialize_body()
        self.body_len = len(body)

        # Build the common frame header.
        # First, get the flags.
        flags = 0

        for flag, flag_bit in self.defined_flags:
            if flag in self.flags:
                flags |= flag_bit

        header = _STRUCT_HBBBL.pack(
            (self.body_len >> 8) & 0xFFFF,  # Length spread over top 24 bits
            self.body_len & 0xFF,
            self.type,
            flags,
            self.stream_id & 0x7FFFFFFF  # Stream ID is 32 bits.
        )

        return header + body

    def serialize_body(self) -> bytes:
        raise NotImplementedError()

    def parse_body(self, data: bytearray) -> None:
        """
        Given the body of a frame, parses it into frame data. This populates
        the non-header parts of the frame: that is, it does not populate the
        stream ID or flags.

        :param data: A memoryview object containing the body data of the frame.
                     Must not contain *more* data than the length returned by
                     :meth:`parse_frame_header
                     <hyperframe.frame.Frame.parse_frame_header>`.
        """
        raise NotImplementedError()


