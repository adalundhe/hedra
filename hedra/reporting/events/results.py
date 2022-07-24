from hedra.core.engines.types.common.types import RequestTypes
from .types import (
    GraphQLEvent,
    GRPCEvent,
    HTTPEvent,
    HTTP2Event,
    PlaywrightEvent,
    WebsocketEvent
)


results_types = {
    RequestTypes.GRAPHQL: GraphQLEvent,
    RequestTypes.GRPC: GRPCEvent,
    RequestTypes.HTTP: HTTPEvent,
    RequestTypes.HTTP2: HTTP2Event,
    RequestTypes.PLAYWRIGHT: PlaywrightEvent,
    RequestTypes.WEBSOCKET: WebsocketEvent
}