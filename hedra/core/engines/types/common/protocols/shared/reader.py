from asyncio import (
    Future,
    Transport,
    get_event_loop
)
from asyncio.exceptions import LimitOverrunError
from .constants import _DEFAULT_LIMIT


class Reader:

    _source_traceback = None
    __slots__ = (
        '_limit',
        '_loop',
        '_buffer',
        '_eof',
        '_waiter',
        '_exception',
        '_transport',
        '_paused',
        '__weakref__'
    )

    def __init__(self, limit=_DEFAULT_LIMIT, loop=None):
        # The line length limit is  a security feature;
        # it also doubles as half the buffer limit.

        if limit <= 0:
            raise ValueError('Limit cannot be <= 0')

        self._limit = limit
        if loop is None:
            self._loop = get_event_loop()
        else:
            self._loop = loop
        self._buffer = bytearray()
        self._eof = False    # Whether we're done.
        self._waiter: Future = None  # A future used by _wait_for_data()
        self._exception = None
        self._transport: Transport = None
        self._paused = False

    def exception(self):
        return self._exception

    def set_exception(self, exc):
        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

    def set_transport(self, transport):
        # assert self._transport is None, 'Transport already set'
        self._transport = transport

    def _maybe_resume_transport(self):
        if self._paused and len(self._buffer) <= self._limit:
            self._paused = False
            self._transport.resume_reading()

    def feed_eof(self):
        self._eof = True

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

    def at_eof(self):
        """Return True if the buffer is empty and 'feed_eof' was called."""
        return self._eof and not self._buffer

    def feed_data(self, data):
        assert not self._eof, 'feed_data after feed_eof'

        if not data:
            return

        self._buffer.extend(data)
        
        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

        if self._transport and self._paused == False and len(self._buffer) > 2 * self._limit:
            try:
                self._transport.pause_reading()
            except NotImplementedError:
                # The transport can't be paused.
                # We'll just have to buffer all data.
                # Forget the transport so we don't keep trying.
                self._transport = None
            else:
                self._paused = True

    async def _wait_for_data(self, func_name):
        """Wait until feed_data() or feed_eof() is called.
        If stream was paused, automatically resume it.
        """
        # StreamReader uses a future to link the protocol feed_data() method
        # to a read coroutine. Running two read coroutines at the same time
        # would have an unexpected behaviour. It would not possible to know
        # which coroutine would get the next data.
        if self._waiter:
            raise RuntimeError(
                f'{func_name}() called while another coroutine is '
                f'already waiting for incoming data'
            )

        assert not self._eof, '_wait_for_data after EOF'

        # Waiting for data while paused will make deadlock, so prevent it.
        # This is essential for readexactly(n) for case when n > self._limit.
        if self._paused:
            self._paused = False
            self._transport.resume_reading()

        self._waiter = self._loop.create_future()
        try:
            await self._waiter
        finally:
            self._waiter = None

    async def read(self, n=-1):

        if not self._buffer and not self._eof:
            await self._wait_for_data('read')

        data = bytes(self._buffer[:n])
        del self._buffer[:n]

        self._maybe_resume_transport()
        return data

    async def readline_fast(self, sep=b'\n'):
        seplen = len(sep)
        if self._exception is not None:
            raise self._exception
        
        if not self._buffer:

            if self._paused:
                self._paused = False
                self._transport.resume_reading()

            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None

        isep = self._buffer.find(sep)
        if isep < 0:
            chunk = bytes(self._buffer)
            self._buffer.clear()
            return chunk


        chunk = self._buffer[:isep + seplen]
        del self._buffer[:isep + seplen]
        self._maybe_resume_transport()
        return bytes(chunk)

    async def readuntil(self, separator=b'\n'):
        """Read data from the stream until ``separator`` is found.
        On success, the data and separator will be removed from the
        internal buffer (consumed). Returned data will include the
        separator at the end.
        Configured stream limit is used to check result. Limit sets the
        maximal length of data that can be returned, not counting the
        separator.
        If an EOF occurs and the complete separator is still not found,
        an IncompleteReadError exception will be raised, and the internal
        buffer will be reset.  The IncompleteReadError.partial attribute
        may contain the separator partially.
        If the data cannot be read because of over limit, a
        LimitOverrunError exception  will be raised, and the data
        will be left in the internal buffer, so it can be read again.
        """
        seplen = len(separator)

        if self._exception is not None:
            raise self._exception

        # Consume whole buffer except last bytes, which length is
        # one less than seplen. Let's check corner cases with
        # separator='SEPARATOR':
        # * we have received almost complete separator (without last
        #   byte). i.e buffer='some textSEPARATO'. In this case we
        #   can safely consume len(separator) - 1 bytes.
        # * last byte of buffer is first byte of separator, i.e.
        #   buffer='abcdefghijklmnopqrS'. We may safely consume
        #   everything except that last byte, but this require to
        #   analyze bytes of buffer that match partial separator.
        #   This is slow and/or require FSM. For this case our
        #   implementation is not optimal, since require rescanning
        #   of data that is known to not belong to separator. In
        #   real world, separator will not be so long to notice
        #   performance problems. Even when reading MIME-encoded
        #   messages :)

        # `offset` is the number of bytes from the beginning of the buffer
        # where there is no occurrence of `separator`.
        offset = 0

        # Loop until we find `separator` in the buffer, exceed the buffer size,
        # or an EOF has happened.
        while True:
            buflen = len(self._buffer)

            # Check if we now have enough data in the buffer for `separator` to
            # fit.
            if buflen - offset >= seplen:
                isep = self._buffer.find(separator, offset)

                if isep != -1:
                    # `separator` is in the buffer. `isep` will be used later
                    # to retrieve the data.
                    break

                # see upper comment for explanation.
                offset = buflen + 1 - seplen
                if offset > self._limit:
                    raise LimitOverrunError(
                        'Separator is not found, and chunk exceed the limit',
                        offset)

            # Complete message (with full separator) may be present in buffer
            # even when EOF flag is set. This may happen when the last chunk
            # adds data which makes separator be found. That's why we check for
            # EOF *ater* inspecting the buffer.
            if self._eof:
                chunk = bytes(self._buffer)
                self._buffer.clear()
                raise Exception('Connection closed.')

            # _wait_for_data() will resume reading if stream was paused.
            if not self._buffer:
                await self._wait_for_data('readuntil')

        if isep > self._limit:
            raise LimitOverrunError(
                'Separator is found, but chunk is longer than limit', isep)

        chunk = self._buffer[:isep + seplen]
        del self._buffer[:isep + seplen]
        self._maybe_resume_transport()
        return bytes(chunk)

    async def read_headers(self, separator=b'\n'):
        """Read data from the stream until ``separator`` is found.
        On success, the data and separator will be removed from the
        internal buffer (consumed). Returned data will include the
        separator at the end.
        Configured stream limit is used to check result. Limit sets the
        maximal length of data that can be returned, not counting the
        separator.
        If an EOF occurs and the complete separator is still not found,
        an IncompleteReadError exception will be raised, and the internal
        buffer will be reset.  The IncompleteReadError.partial attribute
        may contain the separator partially.
        If the data cannot be read because of over limit, a
        LimitOverrunError exception  will be raised, and the data
        will be left in the internal buffer, so it can be read again.
        """
        headers = {}
        seplen = len(separator)

        while True:

            if self._exception is not None:
                raise self._exception
            
            if not self._buffer:

                if self._paused:
                    self._paused = False
                    self._transport.resume_reading()

                self._waiter = self._loop.create_future()
                try:
                    await self._waiter
                finally:
                    self._waiter = None

            isep = self._buffer.find(separator)
            if isep < 0:
                chunk = bytes(self._buffer)
                self._buffer.clear()

            else:
                chunk = bytes(self._buffer[:isep + seplen])
                del self._buffer[:isep + seplen]
                self._maybe_resume_transport()

            if b':' not in chunk:
                break
            
            decoded = chunk.strip().split(b':',1)

            key, value = decoded
            headers[bytes(key).lower()] = value.strip()
            
        return headers

    async def iter_headers(self, separator=b'\n'):
        """Read data from the stream until ``separator`` is found.
        On success, the data and separator will be removed from the
        internal buffer (consumed). Returned data will include the
        separator at the end.
        Configured stream limit is used to check result. Limit sets the
        maximal length of data that can be returned, not counting the
        separator.
        If an EOF occurs and the complete separator is still not found,
        an IncompleteReadError exception will be raised, and the internal
        buffer will be reset.  The IncompleteReadError.partial attribute
        may contain the separator partially.
        If the data cannot be read because of over limit, a
        LimitOverrunError exception  will be raised, and the data
        will be left in the internal buffer, so it can be read again.
        """
        headers = {}
        seplen = len(separator)

        while True:

            if self._exception is not None:
                raise self._exception
            
            if not self._buffer:

                if self._paused:
                    self._paused = False
                    self._transport.resume_reading()

                self._waiter = self._loop.create_future()
                try:
                    await self._waiter
                finally:
                    self._waiter = None

            isep = self._buffer.find(separator)
            if isep < 0:
                chunk = bytes(self._buffer)
                self._buffer.clear()

            else:
                chunk = bytes(self._buffer[:isep + seplen])
                del self._buffer[:isep + seplen]
                self._maybe_resume_transport()

            if b':' not in chunk:
                break
            
            decoded = chunk.strip().split(b':',1)

            key, value = decoded
            yield key.lower(), value, chunk

    async def __aiter__(self):
        line = await self.readuntil()
        yield line

    async def __anext__(self):
        val = await self.readuntil()
        if val == b'':
            raise StopAsyncIteration
        return val
