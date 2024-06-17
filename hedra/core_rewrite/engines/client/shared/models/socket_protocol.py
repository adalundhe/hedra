import socket


class SocketProtocol:
    DEFAULT = socket.SOCK_STREAM
    HTTP2 = socket.SOCK_STREAM
    HTTP3 = socket.SOCK_DGRAM
    UDP = socket.SOCK_DGRAM
    NONE = None
