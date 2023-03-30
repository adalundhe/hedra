from hedra.core.engines.types.common.types import RequestTypes
from .types import (
    GraphQLProcessedResult,
    GraphQLHTTP2ProcessedResult,
    GRPCProcessedResult,
    HTTPProcessedResult,
    HTTP2ProcessedResult,
    HTTP3ProcessedResult,
    PlaywrightProcessedResult,
    TaskProcessedResult,
    UDPProcessedResult,
    WebsocketProcessedResult,
)



results_types = {
    RequestTypes.GRAPHQL: GraphQLProcessedResult,
    RequestTypes.GRAPHQL_HTTP2: GraphQLHTTP2ProcessedResult,
    RequestTypes.GRPC: GRPCProcessedResult,
    RequestTypes.HTTP: HTTPProcessedResult,
    RequestTypes.HTTP2: HTTP2ProcessedResult,
    RequestTypes.HTTP3: HTTP3ProcessedResult,
    RequestTypes.PLAYWRIGHT: PlaywrightProcessedResult,
    RequestTypes.TASK: TaskProcessedResult,
    RequestTypes.UDP: UDPProcessedResult,
    RequestTypes.WEBSOCKET: WebsocketProcessedResult
}

