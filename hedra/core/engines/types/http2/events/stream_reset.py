from .base_event import BaseEvent

class StreamReset(BaseEvent):
    event_type='STREAM_RESET'
    __slots__ = ('stream_id', 'error_code', 'remote_reset')
    """
    The StreamReset event is fired in two situations. The first is when the
    remote party forcefully resets the stream. The second is when the remote
    party has made a protocol error which only affects a single stream. In this
    case, h2 will terminate the stream early and return this event.

    .. versionchanged:: 2.0.0
       This event is now fired when h2 automatically resets a stream.
    """
    def __init__(self):
        #: The Stream ID of the stream that was reset.
        self.stream_id = None

        #: The error code given. Either one of :class:`ErrorCodes
        #: <h2.errors.ErrorCodes>` or ``int``
        self.error_code = None

        #: Whether the remote peer sent a RST_STREAM or we did.
        self.remote_reset = True

    def __repr__(self):
        return "<StreamReset stream_id:%s, error_code:%s, remote_reset:%s>" % (
            self.stream_id, self.error_code, self.remote_reset
        )
