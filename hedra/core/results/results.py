from hedra.core.engines.types.common.types import RequestTypes
from .types import (
    GraphQLResult,
    GRPCResult,
    HTTPResult,
    HTTP2Result,
    PlaywrightResult,
    WebsocketResult
)


results_types = {
    RequestTypes.GRAPHQL: GraphQLResult,
    RequestTypes.GRPC: GRPCResult,
    RequestTypes.HTTP: HTTPResult,
    RequestTypes.HTTP2: HTTP2Result,
    RequestTypes.PLAYWRIGHT: PlaywrightResult,
    RequestTypes.WEBSOCKET: WebsocketResult
}