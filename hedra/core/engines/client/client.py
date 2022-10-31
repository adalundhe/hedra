from asyncio import Future
from types import UnionType
from typing import Dict, Generic, Iterable, TypeVar, Union
from typing_extensions import TypeVarTuple, Unpack

from datadog import initialize

from hedra.core.engines.types.common.types import RequestTypes
from .store import ActionsStore
from .config import Config
from .types import  (
    HTTPClient,
    HTTP2Client,
    GRPCClient,
    GraphQLClient,
    GraphQLHTTP2Client,
    WebsocketClient,
    PlaywrightClient,
    TaskClient,
    UDPClient
)

from .plugins_store import PluginsStore


T = TypeVarTuple('T')


class Client(Generic[Unpack[T]]):

    def __init__(self) -> None:
        self.next_name = None
        self.intercept = False
        self._config: Config = None
        self._http = HTTPClient
        self._http2 = HTTP2Client
        self._grpc = GRPCClient
        self._graphql = GraphQLClient
        self._graphqlh2 = GraphQLHTTP2Client
        self._websocket = WebsocketClient
        self._playwright = PlaywrightClient
        self._udp = UDPClient
        self._task = TaskClient

        self.clients = {}
        self._plugin = PluginsStore[Unpack[T]]()

        self.actions = ActionsStore()

    def __getitem__(self, key: str):
        return self.clients.get(key)

    def __setitem__(self, key, value):
        self.clients[key] = value

    def get_waiters(self) -> Iterable[Future]:
        for session in self.clients.values():
            if session.waiter:
                yield session.waiter

    @property
    def plugin(self) -> Dict[str, Union[Unpack[T]]]:
        self._plugin._config = self._config
        self._plugin.actions = self.actions
        self._plugin.actions.waiter = self.actions.waiter
        self._plugin.actions.current_stage = self.actions.current_stage
        self._plugin.intercept = self.intercept
        self._plugin.next_name = self.next_name

        return self._plugin

    @property
    def http(self):
        if self._http.initialized is False:
            self._http = self._http(self._config)
            self._http.actions = self.actions
            self.clients[RequestTypes.HTTP] = self._http

        self._http.next_name = self.next_name
        self._http.intercept = self.intercept
        return self._http

    @property
    def http2(self):
        if self._http2.initialized is False:
            self._http2 = self._http2(self._config)
            self._http2.actions = self.actions
            self.clients[RequestTypes.HTTP2] = self._http2
       
        self._http2.next_name = self.next_name
        self._http2.intercept = self.intercept
        return self._http2

    @property
    def grpc(self):
        if self._grpc.initialized is False:
            self._grpc = self._grpc(self._config)
            self._grpc.actions = self.actions
            self.clients[RequestTypes.GRPC] = self._grpc

        self._grpc.next_name = self.next_name
        self._grpc.intercept = self.intercept
        return self._grpc

    @property
    def graphql(self):
        if self._graphql.initialized is False:
            self._graphql = self._graphql(self._config)
            self._graphql.actions = self.actions
            self.clients[RequestTypes.GRAPHQL] = self._graphql

        self._graphql.next_name = self.next_name
        self._graphql.intercept = self.intercept
        return self._graphql

    @property
    def graphqlh2(self):
        if self._graphqlh2.initialized is False:
            self._graphqlh2 = self._graphqlh2(self._config)
            self._graphqlh2.actions = self.actions
            self.clients[RequestTypes.GRAPHQL_HTTP2] = self._graphqlh2

        self._graphqlh2.next_name = self.next_name
        self._graphqlh2.intercept = self.intercept
        return self._graphql

    @property
    def websocket(self):
        if self._websocket.initialized is False:
            self._websocket = self._websocket(self._config)
            self._websocket.actions = self.actions
            self.clients[RequestTypes.WEBSOCKET] = self._websocket

        self._websocket.next_name = self.next_name
        self._websocket.intercept = self.intercept
        return self._websocket

    @property
    def playwright(self):
        if self._playwright.initialized is False:
            self._playwright = self._playwright(self._config)
            self._playwright.actions = self.actions
            self.clients[RequestTypes.PLAYWRIGHT] = self._playwright

        self._playwright.next_name = self.next_name
        self._playwright.intercept = self.intercept
        return self._playwright

    @property
    def udp(self):
        if self._udp.initialized is False:
            self._udp = self._udp(self._config)
            self._udp.actions = self.actions
            self.clients[RequestTypes.UDP] = self._udp

        self._udp.next_name = self.next_name
        self._udp.intercept = self.intercept
        return self._udp
    
    @property
    def task(self):
        if self._task.initialized is False:
            self._task = self._task(self._config)
            self.clients[RequestTypes.TASK] = self._task

        self._task.next_name = self.next_name
        return self._task


        


        
        
