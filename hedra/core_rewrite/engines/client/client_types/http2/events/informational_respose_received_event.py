from .base_event import BaseEvent


class InformationalResponseReceived(BaseEvent):
    event_type='INFORMATIONAL_RESPONSE_RECEIVED'
    __slots__ = (
        'stream_id',
        'headers',
        'priority_updated'
    )
    """
    The InformationalResponseReceived event is fired when an informational
    response (that is, one whose status code is a 1XX code) is received from
    the remote peer.

    The remote peer may send any number of these, from zero upwards. These
    responses are most commonly sent in response to requests that have the
    ``expect: 100-continue`` header field present. Most users can safely
    ignore this event unless you are intending to use the
    ``expect: 100-continue`` flow, or are for any reason expecting a different
    1XX status code.

    .. versionadded:: 2.2.0

    .. versionchanged:: 2.3.0
       Changed the type of ``headers`` to :class:`HeaderTuple
       <hpack:hpack.HeaderTuple>`. This has no effect on current users.

    .. versionchanged:: 2.4.0
       Added ``priority_updated`` property.
    """
    def __init__(self):
        #: The Stream ID for the stream this informational response was made
        #: on.
        self.stream_id = None

        #: The headers for this informational response.
        self.headers = None

        #: If this response also had associated priority information, the
        #: associated :class:`PriorityUpdated <h2.events.PriorityUpdated>`
        #: event will be available here.
        #:
        #: .. versionadded:: 2.4.0
        self.priority_updated = None

    def __repr__(self):
        return "<InformationalResponseReceived stream_id:%s, headers:%s>" % (
            self.stream_id, self.headers
        )