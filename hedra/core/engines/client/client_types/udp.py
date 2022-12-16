import asyncio
from typing import Dict, Iterator, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.udp import UDPAction, UDPResult, MercuryUDPClient
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.logging import HedraLogger
from .base_client import BaseClient


class UDPClient(BaseClient[MercuryUDPClient, UDPAction, UDPResult]):

    def __init__(self, config: Config) -> None:
        super().__init__()

        self.session = MercuryUDPClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections
        )
        self.request_type = RequestTypes.UDP
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

    async def receive(
        self,
        url: str, 
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        request = UDPAction(
            self.next_name,
            url,
            wait_for_response=True,
            data=None,
            user=user,
            tags=tags             
        )

        return await self._execute_action(request)

    async def send(
        self,
        url: str, 
        wait_for_resonse: bool = False,
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = []
    ):

        request = UDPAction(
            self.next_name,
            url,
            wait_for_response=wait_for_resonse,
            data=data,
            user=user,
            tags=tags           
        )

        return await self._execute_action(request)

    