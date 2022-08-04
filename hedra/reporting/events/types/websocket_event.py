from hedra.core.engines.types.websocket import WebsocketResult
from .http_event import HTTPEvent


class WebsocketEvent(HTTPEvent):

    def __init__(self, result: WebsocketResult) -> None:
        super(
            WebsocketEvent,
            self
        ).__init__(result)