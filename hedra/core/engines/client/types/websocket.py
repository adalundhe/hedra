import asyncio
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.websocket import (
    MercuryWebsocketClient,
    WebsocketAction
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.client.store import ActionsStore
from .base_client import BaseClient


class WebsocketClient(BaseClient):

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
        if self.session.registered.get(self.next_name) is None:
            request = WebsocketAction(
                self.next_name,
                url,
                method='GET',
                headers=headers,
                data=None,
                user=user,
                tags=tags
            )

            result = await self.session.prepare(request)
            if isinstance(result, Exception):
                raise result
            
            if self.intercept:
                self.actions.store(self.next_name, request, self.session)

                loop = asyncio.get_event_loop()
                self.waiter = loop.create_future()
                await self.waiter

        return self.session

    async def send(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = []
    ):

        if self.session.registered.get(self.next_name) is None:
            request = WebsocketAction(
                self.next_name,
                url,
                method='POST',
                headers=headers,
                data=data,
                user=user,
                tags=tags
            )

            result = await self.session.prepare(request)
            if isinstance(result, Exception):
                raise result
                
            if self.intercept:
                self.actions.store(self.next_name, request, self.session)
                
                loop = asyncio.get_event_loop()
                self.waiter = loop.create_future()
                await self.waiter

        return self.session