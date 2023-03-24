from typing import Any, Tuple, Dict
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.engines.types.websocket import WebsocketResult
from .http_processed_result import HTTPProcessedResult


class WebsocketProcessedResult(HTTPProcessedResult):

    def __init__(
        self, 
        stage: Any, 
        result: WebsocketResult
    ) -> None:
        super(
            WebsocketProcessedResult,
            self
        ).__init__(
            stage,
            result
        )