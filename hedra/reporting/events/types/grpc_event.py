from hedra.core.engines.types.grpc import GRPCResult
from .http2_event import HTTP2Event


class GRPCEvent(HTTP2Event):

    def __init__(self, result: GRPCResult) -> None:
        super().__init__(result)