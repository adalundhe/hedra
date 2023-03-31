from typing import Any
from hedra.core.engines.types.grpc import GRPCResult
from .http2_processed_result import HTTP2ProcessedResult


class GRPCProcessedResult(HTTP2ProcessedResult):

    def __init__(
        self, 
        stage: str, 
        result: GRPCResult
    ) -> None:
        super(
            GRPCProcessedResult,
            self
        ).__init__(
            stage,
            result
        )