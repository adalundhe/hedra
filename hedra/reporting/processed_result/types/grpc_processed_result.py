from typing import Any, Tuple, Dict
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.engines.types.grpc import GRPCResult
from .http2_processed_result import HTTP2ProcessedResult


class GRPCProcessedResult(HTTP2ProcessedResult):

    def __init__(
        self, 
        stage: Any, 
        result: GRPCResult
    ) -> None:
        super(
            GRPCProcessedResult,
            self
        ).__init__(
            stage,
            result
        )