import asyncio
from typing import Union
from hedra.core.engines import Engine
from hedra.parsing.actions.types.custom_action import CustomAction
from hedra.parsing.actions.types.graphql_action import GraphQLAction
from hedra.parsing.actions.types.grpc_action import GrpcAction
from hedra.parsing.actions.types.http_action import HttpAction
from hedra.parsing.actions.types.playwright_action import PlaywrightAction
from hedra.parsing.actions.types.websocket_action import WebsocketAction


class AbstractEngine:

    def __init__(self, config={}) -> None:
        self.engine_type = config.get('engine_type')
        self._engine = Engine(config, None)
        self.session = None

    async def setup_engine(self, session):
        self._engine.engine.session = session

    async def execute(self, action: Union[CustomAction, HttpAction, GraphQLAction, GrpcAction, PlaywrightAction, WebsocketAction]):
        return await self._engine.engine.execute(action)

    async def close(self):
        try:
            await self._engine.close()
        except Exception:
            pass
