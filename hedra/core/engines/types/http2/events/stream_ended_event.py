from .base_event import BaseEvent


class StreamEnded(BaseEvent):
    event_type='STREAM_ENDED'
    __slots__ = ('stream_id')
    """
    The StreamEnded event is fired whenever a stream is ended by a remote
    party. The stream may not be fully closed if it has not been closed
    locally, but no further data or headers should be expected on that stream.
    """
    def __init__(self):
        #: The Stream ID of the stream that was closed.
        self.stream_id = None

    def __repr__(self):
        return "<StreamEnded stream_id:%s>" % self.stream_id
