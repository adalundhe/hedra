from hedra.core_rewrite.engines.client.shared.models import (
    RequestType,
    SocketProtocol,
    SocketType,
)


class ProtocolMap:
    def __init__(self) -> None:
        self.address_families = {
            RequestType.HTTP: SocketType.DEFAULT,
            RequestType.HTTP2: SocketType.HTTP2,
            RequestType.HTTP3: SocketType.HTTP3,
            RequestType.WEBSOCKET: SocketType.DEFAULT,
            RequestType.GRAPHQL: SocketType.DEFAULT,
            RequestType.GRAPHQL_HTTP2: SocketType.HTTP2,
            RequestType.GRPC: SocketType.HTTP2,
            RequestType.UDP: SocketType.UDP,
            RequestType.PLAYWRIGHT: SocketType.NONE,
        }

        self.protocols = {
            RequestType.HTTP: SocketProtocol.DEFAULT,
            RequestType.HTTP2: SocketProtocol.HTTP2,
            RequestType.HTTP3: SocketProtocol.HTTP3,
            RequestType.WEBSOCKET: SocketProtocol.DEFAULT,
            RequestType.GRAPHQL: SocketProtocol.DEFAULT,
            RequestType.GRAPHQL_HTTP2: SocketType.HTTP2,
            RequestType.GRPC: SocketProtocol.HTTP2,
            RequestType.UDP: SocketProtocol.UDP,
            RequestType.PLAYWRIGHT: SocketProtocol.NONE,
        }

    def __getitem__(self, key: RequestType) -> SocketType:
        return self.address_families.get(key), self.protocols.get(key)
