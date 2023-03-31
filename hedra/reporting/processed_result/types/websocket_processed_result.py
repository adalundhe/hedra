from typing import Any
from hedra.core.engines.types.websocket import WebsocketResult
from .http_processed_result import HTTPProcessedResult


class WebsocketProcessedResult(HTTPProcessedResult):

    def __init__(
        self, 
        stage: str, 
        result: WebsocketResult
    ) -> None:
        super(
            WebsocketProcessedResult,
            self
        ).__init__(
            stage,
            result
        )