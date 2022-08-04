from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.result import HTTPResult
from .action import WebsocketAction


class WebsocketResult(HTTPResult):

    def __init__(self, action: WebsocketAction, error: Exception = None) -> None:
        super().__init__(action, error)      
        self.type = RequestTypes.WEBSOCKET