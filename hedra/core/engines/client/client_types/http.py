import asyncio
from typing import Dict, Iterator, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.logging import HedraLogger
from .base_client import BaseClient


class HTTPClient(BaseClient):

    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryHTTPClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections
        )
        self.request_type = RequestTypes.HTTP
        self.client_type = self.request_type.capitalize()

        self.next_name = None
        self.intercept = False
        self.waiter = None
        self.actions: ActionsStore = None
        self.registered = {}

        self.logger = HedraLogger()
        self.logger.initialize()

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def get(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int = 10
    ):

        request = HTTPAction(
            self.next_name,
            url,
            method='GET',
            headers=headers,
            data=None,
            user=user,
            tags=tags,
            redirects=redirects             
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

        return await self.session.execute_prepared_request(request)

    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int = 10
    ):

        request = HTTPAction(
            self.next_name,
            url,
            method='POST',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects           
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

        return await self.session.execute_prepared_request(request)

    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int = 10
    ):

        request = HTTPAction(
            self.next_name,
            url,
            method='PUT',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects
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

        return await self.session.execute_prepared_request(request)

    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int = 10
    ):

        request = HTTPAction(
            self.next_name,
            url,
            method='PATCH',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects
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

        return await self.session.execute_prepared_request(request)

    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int = 10
    ):
        request = HTTPAction(
            self.next_name,
            url,
            method='DELETE',
            headers=headers,
            data=None,
            user=user,
            tags=tags,
            redirects=redirects
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

        return await self.session.execute_prepared_request(request)
