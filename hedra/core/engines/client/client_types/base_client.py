import uuid
import asyncio
from typing import Generic, TypeVar
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types import (
    MercuryGraphQLClient,
    MercuryGraphQLHTTP2Client,
    MercuryGRPCClient,
    MercuryHTTP2Client,
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryTaskRunner,
    MercuryUDPClient,
    MercuryWebsocketClient
)
from hedra.core.engines.types.graphql import GraphQLAction, GraphQLResult
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Action, GraphQLHTTP2Result
from hedra.core.engines.types.grpc import GRPCAction, GRPCResult
from hedra.core.engines.types.http import HTTPAction, HTTPResult
from hedra.core.engines.types.http2 import HTTP2Action, HTTP2Result
from hedra.core.engines.types.playwright import PlaywrightCommand, PlaywrightResult
from hedra.core.engines.types.task import Task, TaskResult
from hedra.core.engines.types.udp import UDPAction, UDPResult
from hedra.core.engines.types.websocket import WebsocketAction, WebsocketResult
from hedra.core.engines.client.store import ActionsStore
from hedra.logging import HedraLogger

S = TypeVar(
    'S',
    MercuryGraphQLClient,
    MercuryGraphQLHTTP2Client,
    MercuryGRPCClient,
    MercuryHTTPClient,
    MercuryHTTP2Client,
    MercuryPlaywrightClient,
    MercuryTaskRunner,
    MercuryUDPClient,
    MercuryWebsocketClient
)
A = TypeVar(
    'A',
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    PlaywrightCommand,
    Task,
    UDPAction,
    WebsocketAction
)
R = TypeVar(
    'R',
    GraphQLResult,
    GraphQLHTTP2Result,
    GRPCResult,
    HTTPResult,
    HTTP2Result,
    PlaywrightResult,
    TaskResult,
    UDPResult,
    WebsocketResult
)


class BaseClient(Generic[S, A, R]):
    initialized=False
    setup=False

    def __init__(self) -> None:
        self.initialized = True
        self.metadata_string: str = None
        self.client_id = str(uuid.uuid4())
        self.session: S =  None
        self.request_type :RequestTypes = None
        self.client_type: str = None

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

        self.logger = HedraLogger()
        self.logger.initialize()

    async def _execute_action(self, action: A) -> R:
        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Preparing Action - {action.name}:{action.action_id}'
        )
        await self.session.prepare(action)

        await self.logger.filesystem.aio['hedra.core'].debug(
            f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Prepared Action - {action.name}:{action.action_id}'
        )

        if self.intercept:
            await self.logger.filesystem.aio['hedra.core'].debug(
                f'{self.metadata_string} - {self.client_type} Client {self.client_id} - Initiating suspense for Action - {action.name}:{action.action_id} - and storing'
            )
            self.actions.store(self.next_name, action, self.session)
            
            loop = asyncio.get_event_loop()
            self.waiter = loop.create_future()
            await self.waiter

        return await self.session.execute_prepared_request(action)