from typing import Any, Tuple, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.grpc import GRPCResult
from .http2_event import HTTP2Event


class GRPCEvent(HTTP2Event):

    def __init__(
        self, 
        stage: Any, 
        result: GRPCResult
    ) -> None:
        super().__init__(
            stage,
            result
        )