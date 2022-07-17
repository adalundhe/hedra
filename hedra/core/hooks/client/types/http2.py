from typing import Dict, List, Union, Iterator
from types import FunctionType

from sklearn.covariance import oas
from hedra.core.hooks.client.config import Config
from hedra.core.engines.types.http2.client import MercuryHTTP2Client
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.common.request import Request
from .base_client import BaseClient


class HTTP2Client(BaseClient):
    
    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryHTTP2Client(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                connect_timeout=config.connect_timeout,
                total_timeout=config.request_timeout
            ),
            reset_connections=config.options.get('reset_connections')
        )
        self.request_type = RequestTypes.HTTP2
        self.next_name = None

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def get(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        user: str = None,
        tags: List[Dict[str, str]] = [],
        checks: List[FunctionType]=[]
    ):
        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='GET',
                    headers=headers,
                    params=params,
                    payload=None,
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if isinstance(result, Exception):
                raise result

        return self.session

    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        checks: List[FunctionType]=[]
    ):
        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='POST',
                    headers=headers,
                    params=params,
                    payload=data,
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if isinstance(result, Exception):
                raise result
                
        return self.session

    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        checks: List[FunctionType]=[]
    ):

        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='PUT',
                    headers=headers,
                    params=params,
                    payload=data,
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if isinstance(result, Exception):
                raise result

        return self.session

    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        checks: List[FunctionType]=[]
    ):

        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='PATCH',
                    headers=headers,
                    params=params,
                    payload=data,
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if isinstance(result, Exception):
                raise result

        return self.session

    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        user: str = None,
        tags: List[Dict[str, str]] = [],
        checks: List[FunctionType]=[]
    ):

        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='DELETE',
                    headers=headers,
                    params=params,
                    payload=None,
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if isinstance(result, Exception):
                raise result

        return self.session