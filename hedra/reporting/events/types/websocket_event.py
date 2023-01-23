from typing import Any, Tuple, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.websocket import WebsocketResult
from .http_event import HTTPEvent


class WebsocketEvent(HTTPEvent):

    def __init__(
        self, 
        stage: Any, 
        result: WebsocketResult
    ) -> None:
        super(
            WebsocketEvent,
            self
        ).__init__(
            stage,
            result
        )