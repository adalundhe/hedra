from .headers_sent_event import HeadersSent

class ResponseSent(HeadersSent):
    event_type='RESPONSE_SENT'
    """
    The _ResponseSent event is fired whenever response headers are sent
    on a stream.

    This is an internal event, used to determine validation steps on
    outgoing header blocks.
    """
    pass
