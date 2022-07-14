from .headers_sent_event import HeadersSent


class RequestSent(HeadersSent):
    event_type='REQUEST_SENT'
    """
    The _RequestSent event is fired whenever request headers are sent
    on a stream.

    This is an internal event, used to determine validation steps on
    outgoing header blocks.
    """
    pass