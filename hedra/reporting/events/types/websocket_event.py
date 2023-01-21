from typing import Any
from hedra.core.engines.types.websocket import WebsocketResult
from .http_event import HTTPEvent


class WebsocketEvent(HTTPEvent):

    def __init__(self, stage: Any, result: WebsocketResult) -> None:
        super(
            WebsocketEvent,
            self
        ).__init__(
            stage,
            result
        )