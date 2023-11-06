import uuid
import threading
import os
from asyncio import Future
from typing import (
    Dict, 
    Generic, 
    Iterable, 
    Union, 
    Optional,
    Callable,
    Any
)
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.types.common import Timeouts
from hedra.core_rewrite.engines.client.client_types.graphql.client import GraphQLClient
from hedra.core_rewrite.engines.client.client_types.graphql_http2.client import GraphQLHTTP2Client
from hedra.core_rewrite.engines.client.client_types.grpc.client import GRPCClient
from hedra.core_rewrite.engines.client.client_types.http.client import HTTPClient
from hedra.core_rewrite.engines.client.client_types.http2.client import HTTP2Client
from hedra.core_rewrite.engines.client.client_types.http3.client import HTTP3Client
from hedra.core_rewrite.engines.client.client_types.playwright.client import PlaywrightClient
from hedra.core_rewrite.engines.client.client_types.udp.client import UDPClient
from hedra.core_rewrite.engines.client.client_types.websocket.client import WebsocketClient
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.experiments.mutations.types.base.mutation import Mutation
from .store import ActionsStore
from .config import Config
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
        self.suspend = False

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
        self.clients = {}
        self._plugin = PluginsStore[Unpack[T]](self.metadata_string)

        self.actions = ActionsStore(self.metadata_string)
        self.mutations: Dict[str, Mutation] = {}

        self.initialized: Dict[str, bool] = {
            'graphql': False,
            'graphql_http2': False,
            'grpc': False,
            'http': False,
            'http2': False,
            'http3': False,
            'playwright': False,
            'udp': False,
            'websocket': False
        }

        self._engines: Dict[
            str,
            Callable[
                [Config],
                Union[
                    GraphQLClient,
                    GraphQLHTTP2Client,
                    GRPCClient,
                    HTTPClient,
                    HTTP2Client,
                    HTTP3Client,
                    PluginsStore[Unpack[T]],
                    PlaywrightClient,
                    UDPClient,
                    WebsocketClient
                ]
            ]
        ] = {
            'graphql': lambda config: GraphQLClient(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'graphqlh2': lambda config: GraphQLHTTP2Client(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'grpc': lambda config: GRPCClient(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'http': lambda config: HTTPClient(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'http2': lambda config: HTTP2Client(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'http3': lambda config: HTTP3Client(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'plugin': lambda config: self._setup_plugin(config),
            'playwright': lambda config: PlaywrightClient(
                concurrency=config.vus,
                group_size=config.group_size,
                timeouts=config.timeouts
            ),
            'udp': lambda config: UDPClient(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            ),
            'websocket': lambda config: WebsocketClient(
                concurrency=config.vus,
                timeouts=config.timeouts,
                reset_connections=config.reset_connections
            )
        }

    def __getitem__(self, key: str):
        return self.clients.get(key)

    def __setitem__(self, key, value):
        self.clients[key] = value

    def set_mutations(self):
        if self._config.mutations:
            for mutation in self._config.mutations:
                for target in mutation.targets:
                    self.mutations[target] = mutation

    def _setup_plugin(
        self,
        config: Config
    ) -> PluginsStore[Unpack[T]]:
        self._plugin._config = self._config
        self._plugin.actions = self.actions
        self._plugin.metadata_string = self.metadata_string
        self._plugin.actions.waiter = self.actions.waiter
        self._plugin.actions.current_stage = self.actions.current_stage

        return self._plugin
        
                    
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
        self._plugin.suspend = self.suspend
        self._plugin.next_name = self.next_name

        self._plugin.mutations.update(self.mutations)
        self.mutations.update(self._plugin.mutations)

        return self._plugin

    @property
    def http(self) -> HTTPClient:

        if not self.initialized.get('http'):
            self._http = self._http(
                concurrency=self._config.batch_size,
                timeouts=self.timeouts,
                reset_connections=self._config.reset_connections
            )

            self.initialized['http'] = True

            self.clients[RequestTypes.HTTP] = self._http

            self._http.mutations.update(self.mutations)
            self.mutations.update(self._http.mutations)

        self._http.next_name = self.next_name
        self._http.suspend = self.suspend
        return self._http

    @property
    def http2(self) -> HTTP2Client:
        if self._http2.initialized is False:
            self._http2 = self._http2(self._config)
            self._http2.actions = self.actions
            self.clients[RequestTypes.HTTP2] = self._http2

            self._http2.mutations.update(self.mutations)
            self.mutations.update(self._http2.mutations)

        self._http2.next_name = self.next_name
        self._http2.suspend = self.suspend
        return self._http2

    @property
    def http3(self) -> HTTP3Client:
        if self._http3.initialized is False:
            self._http3 = self._http3(self._config)
            self._http3.actions = self.actions
            self.clients[RequestTypes.HTTP3] = self._http3

            self._http3.mutations.update(self.mutations)
            self.mutations.update(self._http3.mutations)
       
        self._http3.next_name = self.next_name
        self._http3.suspend = self.suspend
        return self._http3

    @property
    def grpc(self) -> GRPCClient:
        if self._grpc.initialized is False:
            self._grpc = self._grpc(self._config)
            self._grpc.actions = self.actions
            self.clients[RequestTypes.GRPC] = self._grpc

            self._grpc.mutations.update(self.mutations)
            self.mutations.update(self._grpc.mutations)

        self._grpc.next_name = self.next_name
        self._grpc.suspend = self.suspend
        return self._grpc

    @property
    def graphql(self) -> GraphQLClient:
        if self._graphql.initialized is False:
            self._graphql = self._graphql(self._config)
            self._graphql.actions = self.actions
            self.clients[RequestTypes.GRAPHQL] = self._graphql

            self._graphql.mutations.update(self.mutations)
            self.mutations.update(self._graphql.mutations)

        self._graphql.next_name = self.next_name
        self._graphql.suspend = self.suspend
        return self._graphql

    @property
    def graphqlh2(self) -> GraphQLHTTP2Client:

        if self._config is None:
            self._config = config_registry.pop()

        if self._graphqlh2.initialized is False:
            self._graphqlh2 = self._graphqlh2(self._config)
            self._graphqlh2.actions = self.actions
            self.clients[RequestTypes.GRAPHQL_HTTP2] = self._graphqlh2

            self._graphqlh2.mutations.update(self.mutations)
            self.mutations.update(self._graphqlh2.mutations)

        self._graphqlh2.next_name = self.next_name
        self._graphqlh2.suspend = self.suspend
        return self._graphql

    @property
    def playwright(self) -> PlaywrightClient:
        if self._playwright.initialized is False:
            self._playwright = self._playwright(self._config)
            self._playwright.actions = self.actions
            self.clients[RequestTypes.PLAYWRIGHT] = self._playwright

            self._playwright.mutations.update(self.mutations)
            self.mutations.update(self._playwright.mutations)

        self._playwright.next_name = self.next_name
        self._playwright.suspend = self.suspend
        return self._playwright

    @property
    def udp(self) -> UDPClient:
        if self._udp.initialized is False:
            self._udp = self._udp(self._config)
            self._udp.actions = self.actions
            self.clients[RequestTypes.UDP] = self._udp

            self._udp.mutations.update(self.mutations)
            self.mutations.update(self._udp.mutations)

        self._udp.next_name = self.next_name
        self._udp.suspend = self.suspend
        return self._udp
    
    @property
    def websocket(self) -> WebsocketClient:
        if self._websocket.initialized is False:
            self._websocket = self._websocket(self._config)
            self._websocket.actions = self.actions
            self.clients[RequestTypes.WEBSOCKET] = self._websocket

            self._websocket.mutations.update(self.mutations)
            self.mutations.update(self._websocket.mutations)

        self._websocket.next_name = self.next_name
        self._websocket.suspend = self.suspend
        return self._websocket
    
    async def prepare(
        self,
        hook_name: str,
        cached_hook: Dict[str, Any]
    ):
        engine_type: RequestTypes = cached_hook.get('engine')

        if not self.initialized.get(engine_type):
            engine = self._engines.get(engine)(
                self._config
            )

            self.clients[engine_type] = engine
            self.initialized[engine_type] = True

        else:
            engine = self.clients[engine_type]