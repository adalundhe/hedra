import asyncio
from types import FunctionType
from typing import Dict, Iterator, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
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
        self.next_name = None
        self.intercept = False
        self.waiter = None
        self.actions: ActionsStore = None
        self.registered = {}

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def get(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        if self.session.registered.get(self.next_name) is None:

            request = HTTPAction(
                self.next_name,
                url,
                method='GET',
                headers=headers,
                data=None,
                user=user,
                tags=tags             
            )

            await self.session.prepare(request)

            if self.intercept:
                self.actions.store(self.next_name, request, self.session)
                
                loop = asyncio.get_event_loop()
                self.waiter = loop.create_future()
                await self.waiter

        return await self.session.execute_prepared_request(
            self.session.registered.get(self.next_name)
        )

    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):
        if self.session.registered.get(self.next_name) is None:
            request = HTTPAction(
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
    
        return await self.session.execute_prepared_request(
            self.session.registered.get(self.next_name)
        )

    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        if self.session.registered.get(self.next_name) is None:
            request = HTTPAction(
                self.next_name,
                url,
                method='PUT',
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

        return await self.session.execute_prepared_request(
            self.session.registered.get(self.next_name)
        )

    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        if self.session.registered.get(self.next_name) is None:
            request = HTTPAction(
                self.next_name,
                url,
                method='PATCH',
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

        return await self.session.execute_prepared_request(
            self.session.registered.get(self.next_name)
        )

    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        if self.session.registered.get(self.next_name) is None:
            request = HTTPAction(
                self.next_name,
                url,
                method='DELETE',
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

        return await self.session.execute_prepared_request(
            self.session.registered.get(self.next_name)
        )
