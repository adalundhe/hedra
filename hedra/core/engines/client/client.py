from inspect import isclass
from typing import Any

from hedra.core.engines.types.common.types import RequestTypes
from .store import ActionsStore
from .config import Config
from .types import  (
    HTTPClient,
    HTTP2Client,
    GRPCClient,
    GraphQLClient,
    WebsocketClient,
    PlaywrightClient
)



class Client:

    def __init__(self) -> None:
        self.next_name = None
        self.intercept = False
        self._config: Config = None
        self._http = HTTPClient
        self._http2 = HTTP2Client
        self._grpc = GRPCClient
        self._graphql = GraphQLClient
        self._websocket = WebsocketClient
        self._playwright = PlaywrightClient

        self.clients = {}
        self.actions = ActionsStore()

    def __getitem__(self, key: str):
        return self.clients.get(key)

    def __setitem__(self, key, value):
        self.clients[key] = value

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
            self._http.actions = self.actions
            self.clients[RequestTypes.HTTP2] = self._http2
       
        self._http2.next_name = self.next_name
        self._http.intercept = self.intercept
        return self._http2

    @property
    def grpc(self):
        self._grpc.next_name = self.next_name
        if self._grpc.initialized is False:
            self._grpc = self._grpc(self._config)
            self._http.actions = self.actions
            self.clients[RequestTypes.GRPC] = self._grpc

        self._grpc.next_name = self.next_name
        self._grpc.intercept = self.intercept
        return self._grpc

    @property
    def graphql(self):
        self._graphql.next_name = self.next_name
        if self._graphql.initialized is False:
            self._graphql = self._graphql(self._config)
            self._http.actions = self.actions
            self.clients[RequestTypes.GRAPHQL] = self._graphql

        self._graphql.next_name = self.next_name
        self._graphql.intercept = self.intercept
        return self._graphql

    @property
    def websocket(self):
        self._websocket.next_name = self.next_name
        if self._websocket.initialized is False:
            self._websocket = self._websocket(self._config)
            self._http.actions = self.actions
            self.clients[RequestTypes.WEBSOCKET] = self._websocket

        self._websocket.next_name = self.next_name
        self._websocket.intercept = self.intercept
        return self._websocket

    @property
    def playwright(self):
        self._playwright.next_name = self.next_name
        if self._playwright.initialized is False:
            self._playwright = self._playwright(self._config)
            self._http.actions = self.actions
            self.clients[RequestTypes.PLAYWRIGHT] = self._playwright

        self._playwright.next_name = self.next_name
        self._playwright.intercept = self.intercept
        return self._playwright


        


        
        
