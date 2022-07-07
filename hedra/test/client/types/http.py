import inspect
from types import FunctionType
from typing import Dict, Iterator, List, Union
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.common.request import Request
from hedra.core.engines.types.common.types import RequestTypes

class HTTPClient:

    def __init__(self, session: MercuryHTTPClient) -> None:
        self.session = session
        self.request_type = RequestTypes.HTTP
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
                ), 
                checks=[]
            )

            if result and result.error:
                raise result.error


    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):
        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    url,
                    method='POST',
                    headers=headers,
                    params=params,
                    payload=data,
                    user=user,
                    tags=tags,
                    request_type=self.request_type
                )
            )

            if result and result.error:
                raise result.error

    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        self.next_name = inspect.stack()[1][3]
        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    url,
                    method='PUT',
                    headers=headers,
                    params=params,
                    payload=data,
                    user=user,
                    tags=tags,
                    request_type=self.request_type
                )
            )

            if result and result.error:
                raise result.error

    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        self.next_name = inspect.stack()[1][3]
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
                    request_type=self.request_type
                )
            )

            if result and result.error:
                raise result.error

    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {},
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):
        self.next_name = inspect.stack()[1][3]
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
                    request_type=self.request_type
                )
            )

            if result and result.error:
                raise result.error
