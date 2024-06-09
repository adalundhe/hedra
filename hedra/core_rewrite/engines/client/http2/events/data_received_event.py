import binascii
from .base_event import BaseEvent


class DataReceived(BaseEvent):
    event_type='DATA_RECEIVED'

    __slots__ = (
        'stream_id',
        'data',
        'flow_controlled_length',
        'stream_ended'
    )

    def __init__(self):
        #: The Stream ID for the stream this data was received on.
        self.stream_id = None

        #: The data itself.
        self.data = None

        #: The amount of data received that counts against the flow control
        #: window. Note that padding counts against the flow control window, so
        #: when adjusting flow control you should always use this field rather
        #: than ``len(data)``.
        self.flow_controlled_length = None

        #: If this data chunk also completed the stream, the associated
        #: :class:`StreamEnded <h2.events.StreamEnded>` event will be available
        #: here.
        #:
        #: .. versionadded:: 2.4.0
        self.stream_ended = None

    def __repr__(self):
        
        decoded_data = None
        if self.data:
            decoded_data = binascii.hexlify(self.data[:20]).decode('ascii')

        return (
            "<DataReceived stream_id:%s, "
            "flow_controlled_length:%s, "
            "data:%s>" % (
                self.stream_id,
                self.flow_controlled_length,
                decoded_data,
            )
        )