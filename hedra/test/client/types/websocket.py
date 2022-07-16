import inspect
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.websocket.client import MercuryWebsocketClient
from hedra.core.engines.types.common.request import Request
from hedra.core.engines.types.common.types import RequestTypes


class WebsocketClient:

    def __init__(self, session: MercuryWebsocketClient) -> None:
        self.session = session
        self.request_type = RequestTypes.WEBSOCKET
        self.next_name = None

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def listen(
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

    async def send(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        data: Any = None, 
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