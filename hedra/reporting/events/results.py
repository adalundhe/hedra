from typing import List
from hedra.core.engines.types.common.types import RequestTypes
from .types import (
    GraphQLEvent,
    GRPCEvent,
    HTTPEvent,
    HTTP2Event,
    PlaywrightEvent,
    TaskEvent,
    UDPEvent,
    WebsocketEvent,
)



results_types = {
    RequestTypes.GRAPHQL: GraphQLEvent,
    RequestTypes.GRPC: GRPCEvent,
    RequestTypes.HTTP: HTTPEvent,
    RequestTypes.HTTP2: HTTP2Event,
    RequestTypes.PLAYWRIGHT: PlaywrightEvent,
    RequestTypes.TASK: TaskEvent,
    RequestTypes.UDP: UDPEvent,
    RequestTypes.WEBSOCKET: WebsocketEvent
}

