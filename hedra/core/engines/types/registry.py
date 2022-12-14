from .graphql import MercuryGraphQLClient
from .graphql_http2 import MercuryGraphQLHTTP2Client
from .grpc import MercuryGRPCClient
from .http import MercuryHTTPClient
from .http2 import MercuryHTTP2Client
from .playwright import MercuryPlaywrightClient
from .task import MercuryTaskRunner
from .udp import MercuryUDPClient
from .websocket import MercuryWebsocketClient
from .common.types import RequestTypes



registered_engines = {
    RequestTypes.HTTP: lambda concurrency, timeouts, reset_connections: MercuryHTTPClient(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.HTTP2: lambda concurrency, timeouts, reset_connections: MercuryHTTP2Client(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.GRPC: lambda concurrency, timeouts, reset_connections: MercuryGRPCClient(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.GRAPHQL: lambda concurrency, timeouts, reset_connections: MercuryGraphQLClient(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.GRAPHQL_HTTP2: lambda concurrency, timeouts, reset_connections: MercuryGraphQLHTTP2Client(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.PLAYWRIGHT: lambda concurrency, timeouts, reset_connections: MercuryPlaywrightClient(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.TASK: lambda concurrency, timeouts: MercuryTaskRunner(
        concurrency=concurrency,
        timeouts=timeouts
    ),
    RequestTypes.UDP: lambda concurrency, timeouts, reset_connections: MercuryUDPClient(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
    RequestTypes.WEBSOCKET: lambda concurrency, timeouts, reset_connections: MercuryWebsocketClient(
        concurrency=concurrency,
        timeouts=timeouts,
        reset_connections=reset_connections
    ),
}