import socket


class SocketTypes:
    DEFAULT=socket.AF_INET
    HTTP2=socket.AF_INET
    UDP=socket.AF_INET
    NONE=None

class SocketProtocols:
    DEFAULT=socket.SOCK_STREAM
    HTTP2=socket.SOCK_STREAM
    UDP=socket.SOCK_DGRAM
    NONE=None


class RequestTypes:
    HTTP='HTTP'
    HTTP2='HTTP2'
    WEBSOCKET='WEBSOCKET'
    GRAPHQL='GRAPHQL'
    GRAPHQL_HTTP2="GRAPHQL_HTTP2"
    GRPC='GRPC'
    PLAYWRIGHT='PLAYWRIGHT'
    UDP='UDP'
    TASK='TASK'
    CUSTOM='CUSTOM'


class ProtocolMap:

    def __init__(self) -> None:
        self.address_families = {
            RequestTypes.HTTP: SocketTypes.DEFAULT,
            RequestTypes.HTTP2: SocketTypes.HTTP2,
            RequestTypes.WEBSOCKET: SocketTypes.DEFAULT,
            RequestTypes.GRAPHQL: SocketTypes.DEFAULT,
            RequestTypes.GRAPHQL_HTTP2: SocketTypes.HTTP2,
            RequestTypes.GRPC: SocketTypes.HTTP2,
            RequestTypes.UDP: SocketTypes.UDP,
            RequestTypes.PLAYWRIGHT: SocketTypes.NONE
        }

        self.protocols = {
            RequestTypes.HTTP: SocketProtocols.DEFAULT,
            RequestTypes.HTTP2: SocketProtocols.HTTP2,
            RequestTypes.WEBSOCKET: SocketProtocols.DEFAULT,
            RequestTypes.GRAPHQL: SocketProtocols.DEFAULT,
            RequestTypes.GRAPHQL_HTTP2: SocketTypes.HTTP2,
            RequestTypes.GRPC: SocketProtocols.HTTP2,
            RequestTypes.UDP: SocketProtocols.UDP,
            RequestTypes.PLAYWRIGHT: SocketProtocols.NONE
        }

    def __getitem__(self, key: RequestTypes) -> SocketTypes:
        return self.address_families.get(key), self.protocols.get(key)