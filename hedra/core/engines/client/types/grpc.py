import asyncio
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.grpc import (
    MercuryGRPCClient,
    GRPCAction
)
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
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
        tags: List[Dict[str, str]] = []
    ):
        if self.session.registered.get(self.next_name) is None:
            request = GRPCAction(
                self.next_name,
                url,
                method='POST',
                headers=headers,
                data=protobuf,
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