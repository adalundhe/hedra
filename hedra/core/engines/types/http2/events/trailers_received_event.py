from .base_event import BaseEvent


class TrailersReceived(BaseEvent):
    event_type='TRAILERS_RECEIVED'
    __slots__ = ('stream_id', 'headers', 'stream_ended', 'priority_updated')
    """
    The TrailersReceived event is fired whenever trailers are received on a
    stream. Trailers are a set of headers sent after the body of the
    request/response, and are used to provide information that wasn't known
    ahead of time (e.g. content-length). This event carries the HTTP header
    fields that form the trailers and the stream ID of the stream on which they
    were received.

    .. versionchanged:: 2.3.0
       Changed the type of ``headers`` to :class:`HeaderTuple
       <hpack:hpack.HeaderTuple>`. This has no effect on current users.

    .. versionchanged:: 2.4.0
       Added ``stream_ended`` and ``priority_updated`` properties.
    """
    def __init__(self):
        #: The Stream ID for the stream on which these trailers were received.
        self.stream_id = None

        #: The trailers themselves.
        self.headers = None

        #: Trailers always end streams. This property has the associated
        #: :class:`StreamEnded <h2.events.StreamEnded>` in it.
        #:
        #: .. versionadded:: 2.4.0
        self.stream_ended = None

        #: If the trailers also set associated priority information, the
        #: associated :class:`PriorityUpdated <h2.events.PriorityUpdated>`
        #: event will be available here.
        #:
        #: .. versionadded:: 2.4.0
        self.priority_updated = None

    def __repr__(self):
        return "<TrailersReceived stream_id:%s, headers:%s>" % (
            self.stream_id, self.headers
        )