
from hedra.core.engines.types.graphql.result import GraphQLResult
from hedra.core.engines.types.graphql_http2.result import GraphQLHTTP2Result
from hedra.core.engines.types.grpc.result import GRPCResult
from hedra.core.engines.types.http.result import HTTPResult
from hedra.core.engines.types.http2.result import HTTP2Result
from hedra.core.engines.types.http3.result import HTTP3Result
from hedra.core.engines.types.playwright.result import PlaywrightResult
from hedra.core.engines.types.udp.result import UDPResult
from hedra.core.engines.types.websocket.result import WebsocketResult
from typing import Union


Result = Union[
    GraphQLResult,
    GraphQLHTTP2Result,
    GRPCResult,
    HTTPResult,
    HTTP2Result,
    HTTP3Result,
    PlaywrightResult,
    UDPResult,
    WebsocketResult
]
