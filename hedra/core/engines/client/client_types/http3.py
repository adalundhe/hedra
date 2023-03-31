from typing import Dict, List, Union, Iterator
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http3 import(
    MercuryHTTP3Client,
    HTTP3Action,
    HTTP3Result
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.logging import HedraLogger
from .base_client import BaseClient


class HTTP3Client(BaseClient[MercuryHTTP3Client, HTTP3Action, HTTP3Result]):
    
    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryHTTP3Client(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                connect_timeout=config.connect_timeout,
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections
        )
        self.request_type = RequestTypes.HTTP2
        self.client_type = self.request_type.capitalize()

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

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
        redirects: int=3
    ):

        request = HTTP3Action(
            self.next_name,
            url,
            method='GET',
            headers=headers,
            data=None,
            user=user,
            tags=tags,
            redirects=redirects
        )
        
        return await self._execute_action(request)

    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3
    ):

        request = HTTP3Action(
            self.next_name,
            url,
            method='POST',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects
        )
    
        return await self._execute_action(request)


    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        request = HTTP3Action(
            self.next_name,
            url,
            method='PUT',
            headers=headers,
            data=data,
            user=user,
            tags=tags
        )

        return await self._execute_action(request)


    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3
    ):

        request = HTTP3Action(
            self.next_name,
            url,
            method='PATCH',
            headers=headers,
            data=data,
            user=user,
            tags=tags,
            redirects=redirects
        )

        return await self._execute_action(request)


    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        redirects: int=3
    ):

        request = HTTP3Action(
            self.next_name,
            url,
            method='DELETE',
            headers=headers,
            data=None,
            user=user,
            tags=tags,
            redirects=redirects
        )

        return await self._execute_action(request)
