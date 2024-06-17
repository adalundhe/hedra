from typing import Dict, Literal

from .models import RequestType


class RequestTypesMap:
    def __init__(self) -> None:
        self._types: Dict[
            Literal[
                "graphql",
                "graphqlh2",
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
        ]
