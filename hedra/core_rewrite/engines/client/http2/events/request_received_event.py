from .base_event import BaseEvent


class RequestReceived(BaseEvent):
    event_type='REQUEST_RECEIVED'
    __slots__ = (
        'stream_id',
        'headers',
        'stream_ended',
        'priority_updated'
    )
    
    """
    The RequestReceived event is fired whenever request headers are received.
    This event carries the HTTP headers for the given request and the stream ID
    of the new stream.

    .. versionchanged:: 2.3.0
       Changed the type of ``headers`` to :class:`HeaderTuple
       <hpack:hpack.HeaderTuple>`. This has no effect on current users.

    .. versionchanged:: 2.4.0
       Added ``stream_ended`` and ``priority_updated`` properties.
    """
    def __init__(self):
        #: The Stream ID for the stream this request was made on.
        self.stream_id = None

        #: The request headers.
        self.headers = None

        #: If this request also ended the stream, the associated
        #: :class:`StreamEnded <h2.events.StreamEnded>` event will be available
        #: here.
        #:
        #: .. versionadded:: 2.4.0
        self.stream_ended = None

        #: If this request also had associated priority information, the
        #: associated :class:`PriorityUpdated <h2.events.PriorityUpdated>`
        #: event will be available here.
        #:
        #: .. versionadded:: 2.4.0
        self.priority_updated = None

    def __repr__(self):
        return "<RequestReceived stream_id:%s, headers:%s>" % (
            self.stream_id, self.headers
        )