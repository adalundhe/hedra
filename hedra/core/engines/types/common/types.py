import socket


class SocketTypes:
    DEFAULT=0
    HTTP2=socket.AF_INET
    NONE=None

class SocketProtocols:
    DEFAULT=socket.SOCK_STREAM
    HTTP2=socket.SOCK_STREAM
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
        self.address_families = {
            RequestTypes.HTTP: SocketTypes.DEFAULT,
            RequestTypes.HTTP2: SocketTypes.HTTP2,
            RequestTypes.WEBSOCKET: SocketTypes.DEFAULT,
            RequestTypes.GRAPHQL: SocketTypes.DEFAULT,
            RequestTypes.GRPC: SocketTypes.HTTP2,
            RequestTypes.PLAYWRIGHT: SocketTypes.NONE
        }

        self.protocols = {
            RequestTypes.HTTP: SocketProtocols.DEFAULT,
            RequestTypes.HTTP2: SocketProtocols.HTTP2,
            RequestTypes.WEBSOCKET: SocketProtocols.DEFAULT,
            RequestTypes.GRAPHQL: SocketProtocols.DEFAULT,
            RequestTypes.GRPC: SocketProtocols.HTTP2,
            RequestTypes.PLAYWRIGHT: SocketProtocols.NONE
        }

    def __getitem__(self, key: RequestTypes) -> SocketTypes:
        return self.address_families.get(key), self.protocols.get(key)