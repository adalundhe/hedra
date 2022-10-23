# -*- coding: utf-8 -*-
"""
h2/windows
~~~~~~~~~~

Defines tools for managing HTTP/2 flow control windows.

The objects defined in this module are used to automatically manage HTTP/2
flow control windows. Specifically, they keep track of what the size of the
window is, how much data has been consumed from that window, and how much data
the user has already used. It then implements a basic algorithm that attempts
to manage the flow control window without user input, trying to ensure that it
does not emit too many WINDOW_UPDATE frames.
"""
from __future__ import division



# The largest acceptable value for a HTTP/2 flow control window.
LARGEST_FLOW_CONTROL_WINDOW = 2**31 - 1


class WindowManager:

    __slots__ = (
        'max_window_size',
        'current_window_size',
        '_bytes_processed'
    )

    """
    A basic HTTP/2 window manager.

    :param max_window_size: The maximum size of the flow control window.
    :type max_window_size: ``int``
    """
    def __init__(self, max_window_size):
        self.max_window_size = max_window_size
        self.current_window_size = max_window_size
        self._bytes_processed = 0

    def window_consumed(self, size):
        """
        We have received a certain number of bytes from the remote peer. This
        necessarily shrinks the flow control window!

        :param size: The number of flow controlled bytes we received from the
            remote peer.
        :type size: ``int``
        :returns: Nothing.
        :rtype: ``None``
        """
        self.current_window_size -= size

    def window_opened(self, size):
        """
        The flow control window has been incremented, either because of manual
        flow control management or because of the user changing the flow
        control settings. This can have the effect of increasing what we
        consider to be the "maximum" flow control window size.

        This does not increase our view of how many bytes have been processed,
        only of how much space is in the window.

        :param size: The increment to the flow control window we received.
        :type size: ``int``
        :returns: Nothing
        :rtype: ``None``
        """
        self.current_window_size += size

        if self.current_window_size > self.max_window_size:
            self.max_window_size = self.current_window_size

    def process_bytes(self, size):
        """
        The application has informed us that it has processed a certain number
        of bytes. This may cause us to want to emit a window update frame. If
        we do want to emit a window update frame, this method will return the
        number of bytes that we should increment the window by.

        :param size: The number of flow controlled bytes that the application
            has processed.
        :type size: ``int``
        :returns: The number of bytes to increment the flow control window by,
            or ``None``.
        :rtype: ``int`` or ``None``
        """
        
        self._bytes_processed += size
        if not self._bytes_processed:
            return None

        max_increment = (self.max_window_size - self.current_window_size)
        increment = 0

        min_threshold =  (self.current_window_size == 0) and (self._bytes_processed > min(1024, self.max_window_size // 4))
        max_threshold = self._bytes_processed >= (self.max_window_size // 2)

        # Note that, even though we may increment less than _bytes_processed,
        # we still want to set it to zero whenever we emit an increment. This
        # is because we'll always increment up to the maximum we can.
        if min_threshold or max_threshold:
            increment = min(self._bytes_processed, max_increment)
            self._bytes_processed = 0

        self.current_window_size += increment
        return increment