from .base_event import BaseEvent


class WindowUpdated(BaseEvent):
    event_type='WINDOW_UPDATED'
    __slots__ = ('stream_id', 'delta')
    """
    The WindowUpdated event is fired whenever a flow control window changes
    size. HTTP/2 defines flow control windows for connections and streams: this
    event fires for both connections and streams. The event carries the ID of
    the stream to which it applies (set to zero if the window update applies to
    the connection), and the delta in the window size.
    """
    def __init__(self):
        #: The Stream ID of the stream whose flow control window was changed.
        #: May be ``0`` if the connection window was changed.
        self.stream_id = None

        #: The window delta.
        self.delta = None

    def __repr__(self):
        return "<WindowUpdated stream_id:%s, delta:%s>" % (
            self.stream_id, self.delta
        )