from.types import ErrorCodes


class StreamError(Exception):
    error_code = None

class StreamResetException(Exception):
    pass


class NoSuchStreamError(Exception):
    """
    A stream-specific action referenced a stream that does not exist.

    .. versionchanged:: 2.0.0
       Became a subclass of :class:`ProtocolError
       <h2.exceptions.ProtocolError>`
    """
    def __init__(self, stream_id):
        #: The stream ID corresponds to the non-existent stream.
        self.stream_id = stream_id


class StreamClosedError(Exception):
    """
    A more specific form of
    :class:`NoSuchStreamError <h2.exceptions.NoSuchStreamError>`. Indicates
    that the stream has since been closed, and that all state relating to that
    stream has been removed.
    """

    __slots__ = (
        'stream_id',
        'error_code',
        'events'
    )

    def __init__(self, stream_id):
        #: The stream ID corresponds to the nonexistent stream.
        self.stream_id = stream_id

        #: The relevant HTTP/2 error code.
        self.error_code = ErrorCodes.STREAM_CLOSED

        # Any events that internal code may need to fire. Not relevant to
        # external users that may receive a StreamClosedError.
        self._events = []
