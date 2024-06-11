import uuid
from typing import Generic, Optional

from typing_extensions import TypeVarTuple, Unpack

from .config import Config
from .graphql import MercurySyncGraphQLConnection
from .grpc import MercurySyncGRPCConnection
from .http import MercurySyncHTTPConnection
from .http2 import MercurySyncHTTP2Connection
from .http3 import MercurySyncHTTP3Connection
from .udp import MercurySyncUDPConnection
from .websocket import MercurySyncWebsocketConnection

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

        self.graphql = MercurySyncGraphQLConnection(pool_size=config.vus)
        self.grpc = MercurySyncGRPCConnection(pool_size=config.vus)
        self.http = MercurySyncHTTPConnection(pool_size=config.vus)
        self.http2 = MercurySyncHTTP2Connection(pool_size=config.vus)
        self.http3 = MercurySyncHTTP3Connection(pool_size=config.vus)
        self.udp = MercurySyncUDPConnection(pool_size=config.vus)
        self.websocket = MercurySyncWebsocketConnection(pool_size=config.vus)