from typing import Dict, Literal

from .models import RequestType


class RequestTypesMap:
    def __init__(self) -> None:
        self._types: Dict[
            Literal[
                "graphql",
                "graphqlh2",
                "grpc",
                "http",
                "http2",
                "http3",
                "playwright",
                "udp",
                "websocket",
            ],
            Literal[
                RequestType.GRAPHQL,
                RequestType.GRAPHQL_HTTP2,
                RequestType.HTTP,
                RequestType.HTTP2,
                RequestType.HTTP3,
                RequestType.PLAYWRIGHT,
                RequestType.UDP,
                RequestType.WEBSOCKET,
            ],
        ] = {
            "graphql": RequestType.GRAPHQL,
            "graphqlh2": RequestType.GRAPHQL_HTTP2,
            "grpc": RequestType.GRPC,
            "http": RequestType.HTTP,
            "http2": RequestType.HTTP2,
            "http3": RequestType.HTTP3,
            "playwright": RequestType.PLAYWRIGHT,
            "udp": RequestType.UDP,
            "websocket": RequestType.WEBSOCKET,
        }

    def __getitem__(
        self,
        key: Literal[
            "graphql",
            "graphqlh2",
            "http",
            "http2",
            "http3",
            "playwright",
            "udp",
            "websocket",
        ],
    ) -> Literal[
        RequestType.GRAPHQL,
        RequestType.GRAPHQL_HTTP2,
        RequestType.HTTP,
        RequestType.HTTP2,
        RequestType.HTTP3,
        RequestType.PLAYWRIGHT,
        RequestType.UDP,
        RequestType.WEBSOCKET,
    ]:
        return self._types[key]
