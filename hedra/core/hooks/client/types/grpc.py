import asyncio
import inspect
from types import FunctionType
from typing import Any, Dict, List

from requests import request
from hedra.core.hooks.client.config import Config
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.grpc.client import MercuryGRPCClient
from hedra.core.engines.types.common import Timeouts
from hedra.core.hooks.client.store import ActionsStore
from .base_client import BaseClient


class GRPCClient(BaseClient):
    
    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryGRPCClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.options.get('reset_connections')
        )
        self.request_type = RequestTypes.GRPC
        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def request(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        protobuf: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ):
        if self.session.registered.get(self.next_name) is None:
            request = Request(
                self.next_name,
                url,
                method='POST',
                headers=headers,
                payload=protobuf,
                user=user,
                tags=tags,
                checks=checks,
                request_type=self.request_type
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