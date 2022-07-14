from enum import Enum

class StreamClosedBy(Enum):
    SEND_END_STREAM = 0
    RECV_END_STREAM = 1
    SEND_RST_STREAM = 2
    RECV_RST_STREAM = 3