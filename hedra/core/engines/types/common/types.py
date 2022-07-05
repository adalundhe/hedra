import socket


class SocketTypes:
    DEFAULT=0
    HTTP2=socket.AF_INET
    NONE=None


class RequestTypes:
    HTTP='HTTP'
    HTTP2='HTTP2'
    WEBSOCKET='WEBSOCKET'
    GRAPHQL='GRAPHQL'
    GRPC='GRPC'
    PLAYWRIGHT='PLAYWRIGHT'


class ProtocolMap:

    def __init__(self) -> None:
        self.types = {
            RequestTypes.HTTP: SocketTypes.DEFAULT,
            RequestTypes.HTTP2: SocketTypes.HTTP2,
            RequestTypes.WEBSOCKET: SocketTypes.DEFAULT,
            RequestTypes.GRAPHQL: SocketTypes.DEFAULT,
            RequestTypes.GRPC: SocketTypes.HTTP2,
            RequestTypes.PLAYWRIGHT: SocketTypes.NONE
        }

    def __getitem__(self, key: RequestTypes) -> SocketTypes:
        return self.types.get(key)