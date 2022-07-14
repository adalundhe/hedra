# -*- coding: utf-8 -*-
"""
h2/frame_buffer
~~~~~~~~~~~~~~~

A data structure that provides a way to iterate over a byte buffer in terms of
frames.
"""
from .types.attributes import (
    _STRUCT_HBBBL
)
from .types import *
from hyperframe.exceptions import InvalidFrameError, InvalidDataError

from .types import FRAMES



class FrameBuffer:
    """
    This is a data structure that expects to act as a buffer for HTTP/2 data
    that allows iteraton in terms of H2 frames.
    """
    def __init__(self):
        self.data = bytearray()
        self.max_frame_size = 0
        self._headers_buffer = []

    def add_data(self, data):
        """
        Add more data to the frame buffer.

        :param data: A bytestring containing the byte buffer.
        """

        self.data.extend(data)

    # The methods below support the iterator protocol.
    def __iter__(self):
        while len(self.data) >= 9:
            try:
                
                fields = _STRUCT_HBBBL.unpack(self.data[:9])

                # First 24 bits are frame length.
                length = (fields[0] << 8) + fields[1]
                type = fields[2]
                flags = fields[3]
                stream_id = fields[4] & 0x7FFFFFFF

                frame = FRAMES.get(
                    type, 
                    ExtensionFrame(type=type, stream_id=stream_id)

                )(stream_id)

                frame.parse_flags(flags)
            except (InvalidDataError, InvalidFrameError) as e:  # pragma: no cover
                raise Exception(
                    "Received frame with invalid header: %s" % str(e)
                )

            # Next, check that we have enough length to parse the frame body. If
            # not, bail, leaving the frame header data in the buffer for next time.
            if len(self.data) < length + 9:
                break

            # Confirm the frame has an appropriate length.
            # self._validate_frame_length(length)
            if length > self.max_frame_size:
                raise Exception(
                    "Received overlong frame: length %d, max %d" %
                    (length, self.max_frame_size)
                )

            # Try to parse the frame body
            
            frame.parse_body(self.data[9:9+length])
            # At this point, as we know we'll use or discard the entire frame, we
            # can update the data.
            del self.data[:9+length]

            # Pass the frame through the heaer buffer.
            # f = self._update_header_buffer(f)
            is_headers_or_push_promise = frame.frame_type == 'HEADERS' or frame.frame_type == 'PUSHPROMISE'

            if self._headers_buffer:
                stream_id = self._headers_buffer[0].stream_id
                valid_frame = (
                    frame is not None and
                    frame.frame_type == 'CONTINUATION' and
                    frame.stream_id == stream_id
                )

                if valid_frame == False:
                    raise Exception("Invalid frame during header block.")

                # Append the frame to the buffer.
                self._headers_buffer.append(frame)

                # If this is the end of the header block, then we want to build a
                # mutant HEADERS frame that's massive. Use the original one we got,
                # then set END_HEADERS and set its data appopriately. If it's not
                # the end of the block, lose the current frame: we can't yield it.
                if 'END_HEADERS' in frame.flags:
                    frame = self._headers_buffer[0]
                    frame.flags.add('END_HEADERS')

                    frame_data = bytearray()
                    for header_frame in self._headers_buffer:
                        frame_data.extend(header_frame)

                    frame.data = frame_data
                    self._headers_buffer = []

                else:
                    frame = None
            elif is_headers_or_push_promise and 'END_HEADERS' not in frame.flags:
                # This is the start of a headers block! Save the frame off and then
                # act like we didn't receive one.
                self._headers_buffer.append(frame)
                frame = None

            # If we got a frame we didn't understand or shouldn't yield, rather
            # than return None it'd be better if we just tried to get the next
            # frame in the sequence instead. Recurse back into ourselves to do
            # that. This is safe because the amount of work we have to do here is
            # strictly bounded by the length of the buffer.
            if frame:
                yield frame