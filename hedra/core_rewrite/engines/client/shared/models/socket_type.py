import socket


class SocketType:
    DEFAULT = socket.AF_INET
    HTTP2 = socket.AF_INET
    UDP = socket.AF_INET
    HTTP3 = socket.AF_INET6
    NONE = None
