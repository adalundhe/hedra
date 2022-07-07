from typing import Union
from .types import  (
    HTTPClient,
    HTTP2Client,
    GRPCClient,
    GraphQLClient,
    WebsocketClient,
    PlaywrightClient
)

from hedra.core.engines.types import (
    MercuryGraphQLClient,
    MercuryGRPCClient,
    MercuryHTTP2Client,
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryWebsocketClient
)



class Client:

    def __init__(
        self, 
        session: Union[MercuryGraphQLClient , MercuryGRPCClient , MercuryHTTP2Client , MercuryHTTPClient , MercuryPlaywrightClient , MercuryWebsocketClient]
    ) -> None:
        self.session = session
        self.http = HTTPClient(session)
        self.http2 = HTTP2Client(session)
        self.grpc = GRPCClient(session)
        self.graphql = GraphQLClient(session)
        self.websocket = WebsocketClient(session)
        self.playwright = PlaywrightClient(session)

        self._clients = {
            'http': self.http,
            'http2': self.http2,
            'grpc': self.grpc,
            'graphql': self.graphql,
            'websocket': self.websocket,
            'playwright': self.playwright
        }

    def __getitem__(self, key: str):
        return self._clients.get(key)

    def __setitem__(self, key, value):
        self._clients[key] = value
        


        
        
