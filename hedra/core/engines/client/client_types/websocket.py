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
from hedra.logging import HedraLogger
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
        self.client_type = self.request_type.capitalize()

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

        self.logger = HedraLogger()
        self.logger.initialize()

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

        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Preparing Action - {request.name}'
        )
        await self.session.prepare(request)

        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Prepared Action - {request.name}'
        )

        if self.intercept:
            await self.logger.filesystem.aio['hedra.core'].debug(
                f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Initiating suspense for Action - {request.name} - and storing'
            )
            self.actions.store(self.next_name, request, self.session)
            
            loop = asyncio.get_event_loop()
            self.waiter = loop.create_future()
            await self.waiter

        return self.session.execute_prepared_request(request)

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

        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Preparing Action - {request.name}'
        )
        await self.session.prepare(request)

        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Prepared Action - {request.name}'
        )

        if self.intercept:
            await self.logger.filesystem.aio['hedra.core'].debug(
                f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Initiating suspense for Action - {request.name} - and storing'
            )
            self.actions.store(self.next_name, request, self.session)
            
            loop = asyncio.get_event_loop()
            self.waiter = loop.create_future()
            await self.waiter

        return self.session.execute_prepared_request(request)