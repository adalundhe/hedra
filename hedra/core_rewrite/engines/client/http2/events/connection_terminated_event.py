import binascii
from .base_event import BaseEvent



class ConnectionTerminated(BaseEvent):
    event_type='CONNECTION_TERMINATED'

    __slots__ = (
        'error_code',
        'last_stream_id',
        'additional_data'
    )

    """
    The ConnectionTerminated event is fired when a connection is torn down by
    the remote peer using a GOAWAY frame. Once received, no further action may
    be taken on the connection: a new connection must be established.
    """
    def __init__(self):
        #: The error code cited when tearing down the connection. Should be
        #: one of :class:`ErrorCodes <h2.errors.ErrorCodes>`, but may not be if
        #: unknown HTTP/2 extensions are being used.
        self.error_code = None

        #: The stream ID of the last stream the remote peer saw. This can
        #: provide an indication of what data, if any, never reached the remote
        #: peer and so can safely be resent.
        self.last_stream_id = None

        #: Additional debug data that can be appended to GOAWAY frame.
        self.additional_data = None

    def __repr__(self):
        additional_data = b''
        if self.additional_data:
            additional_data = binascii.hexlify(self.additional_data[:20]).decode('ascii')
        return (
            "<ConnectionTerminated error_code:%s, last_stream_id:%s, "
            "additional_data:%s>" % (
                self.error_code,
                self.last_stream_id,
                additional_data
        ))
