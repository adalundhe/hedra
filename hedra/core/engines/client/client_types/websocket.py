import asyncio
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.websocket import (
    MercuryWebsocketClient,
    WebsocketAction,
    WebsocketResult
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.client.store import ActionsStore
from hedra.logging import HedraLogger
from .base_client import BaseClient


class WebsocketClient(BaseClient[MercuryWebsocketClient, WebsocketAction, WebsocketResult]):

    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryWebsocketClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections
        )
        self.request_type = RequestTypes.WEBSOCKET
        self.client_type = self.request_type.capitalize()

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def listen(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None, 
        tags: List[Dict[str, str]] = []  
    ):

        request = WebsocketAction(
            self.next_name,
            url,
            method='GET',
            headers=headers,
            data=None,
            user=user,
            tags=tags
        )

        return await self._execute_action(request)

    async def send(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = []
    ):

        request = WebsocketAction(
            self.next_name,
            url,
            method='POST',
            headers=headers,
            data=data,
            user=user,
            tags=tags
        )

        return await self._execute_action(request)