import asyncio
from collections import defaultdict
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types import (
    MercuryGraphQLClient,
    MercuryGraphQLHTTP2Client,
    MercuryGRPCClient,
    MercuryHTTP2Client,
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryWebsocketClient,
    MercuryUDPClient
)
from hedra.logging import HedraLogger
from typing import Any, Tuple, Union


class ActionsStore:

    def __init__(self, metadata_string: str) -> None:
        self.metadata_string = metadata_string
        self.actions = defaultdict(dict)
        self.sessions = defaultdict(dict)
        self._loop = None
        self.current_stage: str = None
        self.waiter = None
        self.setup_call = None
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    def set_waiter(self, stage: str):

        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        self.waiter = self._loop.create_future()
        self.current_stage = stage

    async def wait_for_ready(self, setup_call):
        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Action Store waiting for Action or Task to notify store it is ready')
        self.setup_call = setup_call
        await self.waiter

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Action Store was notified and is exiting suspension')

    def store(self, request: str, action: Any, session: Any):

        self.actions[self.current_stage][request] = action
        self.sessions[self.current_stage][request] = session

        try:
            self.waiter.set_result(None)
        except asyncio.exceptions.CancelledError:
            pass

    def get(self, stage: str, action_name: str) -> Tuple[BaseAction, Union[MercuryGraphQLClient, MercuryGraphQLHTTP2Client, MercuryGRPCClient, MercuryHTTP2Client, MercuryHTTPClient, MercuryPlaywrightClient, MercuryWebsocketClient, MercuryUDPClient]]:
        action = self.actions.get(
            stage
        ).get(action_name)

        session = self.sessions.get(
            stage
        ).get(action_name)

        return action, session
