from .base_event import BaseEvent


class HeadersSent(BaseEvent):
    event_type='HEADERS_SENT'
    """
    The _HeadersSent event is fired whenever headers are sent.

    This is an internal event, used to determine validation steps on
    outgoing header blocks.
    """
    pass

