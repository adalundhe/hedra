import uuid
import threading
import os
from asyncio import Future
from typing import (
    Dict, 
    Generic, 
    Iterable, 
    Union, 
    Optional
)
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.experiments.mutations.types.base.mutation import Mutation
from .store import ActionsStore
from .config import Config
from .client_types import  (
    HTTPClient,
    HTTP2Client,
    HTTP3Client,
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

config_registry = []


class Client(Generic[Unpack[T]]):

    def __init__(
        self,
        graph_name: str,
        graph_id: str,
        stage_name: str,
        stage_id: str,
        config: Optional[Config]=None
    ) -> None:

        self.client_id = str(uuid.uuid4())
        self.graph_name = graph_name
        self.graph_id = graph_id
        self.stage_name = stage_name
        self.stage_id = stage_id

        self.next_name = None
        self.intercept = False

        self._config: Config = config
        self._http = HTTPClient
        self._http2 = HTTP2Client
        self._http3 = HTTP3Client
        self._grpc = GRPCClient
        self._graphql = GraphQLClient
        self._graphqlh2 = GraphQLHTTP2Client
        self._websocket = WebsocketClient
        self._playwright = PlaywrightClient
        self._udp = UDPClient
        self._task = TaskClient

        self.clients = {}
        self._plugin = PluginsStore[Unpack[T]](self.metadata_string)

        self.actions = ActionsStore(self.metadata_string)
        self.mutations: Dict[str, Mutation] = {}

    def __getitem__(self, key: str):
        return self.clients.get(key)

    def __setitem__(self, key, value):
        self.clients[key] = value

    def get_waiters(self) -> Iterable[Future]:
        for session in self.clients.values():
            if session.waiter:
                yield session.waiter

    def set_mutations(self):
        if self._config.mutations:
            for mutation in self._config.mutations:
                for target in mutation.targets:
                    self.mutations[target] = mutation
                    
    @property
    def thread_id(self) -> int:
        return threading.current_thread().ident

    @property
    def process_id(self) -> int:
        return os.getpid()

    @property
    def metadata_string(self):
        return f'Graph - {self.graph_name}:{self.graph_id} - thread:{self.thread_id} - process:{self.process_id} - Stage: {self.stage_name}:{self.stage_id} - '

    @property
    def plugin(self) -> Dict[str, Union[Unpack[T]]]:
        self._plugin._config = self._config
        self._plugin.actions = self.actions
        self._plugin.metadata_string = self.metadata_string
        self._plugin.actions.waiter = self.actions.waiter
        self._plugin.actions.current_stage = self.actions.current_stage
        self._plugin.intercept = self.intercept
        self._plugin.next_name = self.next_name

        self._plugin.mutations.update(self.mutations)
        self.mutations.update(self._plugin.mutations)

        return self._plugin

    @property
    def http(self):
        if self._http.initialized is False:
            self._http = self._http(self._config)
            self._http.metadata_string = self.metadata_string
            self._http.actions = self.actions
            self.clients[RequestTypes.HTTP] = self._http

            self._http.mutations.update(self.mutations)
            self.mutations.update(self._http.mutations)

        self._http.next_name = self.next_name
        self._http.intercept = self.intercept
        return self._http

    @property
    def http2(self):
        if self._http2.initialized is False:
            self._http2 = self._http2(self._config)
            self._http2.metadata_string = self.metadata_string
            self._http2.actions = self.actions
            self.clients[RequestTypes.HTTP2] = self._http2

            self._http2.mutations.update(self.mutations)
            self.mutations.update(self._http2.mutations)

        self._http2.next_name = self.next_name
        self._http2.intercept = self.intercept
        return self._http2

    @property
    def http3(self):
        if self._http3.initialized is False:
            self._http3 = self._http3(self._config)
            self._http3.metadata_string = self.metadata_string
            self._http3.actions = self.actions
            self.clients[RequestTypes.HTTP3] = self._http3

            self._http3.mutations.update(self.mutations)
            self.mutations.update(self._http3.mutations)
       
        self._http3.next_name = self.next_name
        self._http3.intercept = self.intercept
        return self._http3

    @property
    def grpc(self):
        if self._grpc.initialized is False:
            self._grpc = self._grpc(self._config)
            self._grpc.metadata_string = self.metadata_string
            self._grpc.actions = self.actions
            self.clients[RequestTypes.GRPC] = self._grpc

            self._grpc.mutations.update(self.mutations)
            self.mutations.update(self._grpc.mutations)

        self._grpc.next_name = self.next_name
        self._grpc.intercept = self.intercept
        return self._grpc

    @property
    def graphql(self):
        if self._graphql.initialized is False:
            self._graphql = self._graphql(self._config)
            self._graphql.metadata_string = self.metadata_string
            self._graphql.actions = self.actions
            self.clients[RequestTypes.GRAPHQL] = self._graphql

            self._graphql.mutations.update(self.mutations)
            self.mutations.update(self._graphql.mutations)

        self._graphql.next_name = self.next_name
        self._graphql.intercept = self.intercept
        return self._graphql

    @property
    def graphqlh2(self):

        if self._config is None:
            self._config = config_registry.pop()

        if self._graphqlh2.initialized is False:
            self._graphqlh2 = self._graphqlh2(self._config)
            self._graphqlh2.metadata_string = self.metadata_string
            self._graphqlh2.actions = self.actions
            self.clients[RequestTypes.GRAPHQL_HTTP2] = self._graphqlh2

            self._graphqlh2.mutations.update(self.mutations)
            self.mutations.update(self._graphqlh2.mutations)

        self._graphqlh2.next_name = self.next_name
        self._graphqlh2.intercept = self.intercept
        return self._graphql

    @property
    def websocket(self):
        if self._websocket.initialized is False:
            self._websocket = self._websocket(self._config)
            self._websocket.metadata_string = self.metadata_string
            self._websocket.actions = self.actions
            self.clients[RequestTypes.WEBSOCKET] = self._websocket

            self._websocket.mutations.update(self.mutations)
            self.mutations.update(self._websocket.mutations)

        self._websocket.next_name = self.next_name
        self._websocket.intercept = self.intercept
        return self._websocket

    @property
    def playwright(self):
        if self._playwright.initialized is False:
            self._playwright = self._playwright(self._config)
            self._playwright.metadata_string = self.metadata_string
            self._playwright.actions = self.actions
            self.clients[RequestTypes.PLAYWRIGHT] = self._playwright

            self._playwright.mutations.update(self.mutations)
            self.mutations.update(self._playwright.mutations)

        self._playwright.next_name = self.next_name
        self._playwright.intercept = self.intercept
        return self._playwright

    @property
    def udp(self):
        if self._udp.initialized is False:
            self._udp = self._udp(self._config)
            self._udp.metadata_string = self.metadata_string
            self._udp.actions = self.actions
            self.clients[RequestTypes.UDP] = self._udp

            self._udp.mutations.update(self.mutations)
            self.mutations.update(self._udp.mutations)

        self._udp.next_name = self.next_name
        self._udp.intercept = self.intercept
        return self._udp
    
    @property
    def task(self):
            
        if self._task.initialized is False:
            self._task = self._task(self._config)
            self._task.metadata_string = self.metadata_string
            self.clients[RequestTypes.TASK] = self._task
            
            self._task.mutations.update(self.mutations)
            self.mutations.update(self._task.mutations)

        self._task.next_name = self.next_name
        return self._task


        


        
        
