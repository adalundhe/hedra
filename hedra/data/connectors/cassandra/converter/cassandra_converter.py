from hedra.core.engines.types.graphql import (
    GraphQLAction,
    GraphQLResult
)

from hedra.core.engines.types.graphql_http2 import (
    GraphQLHTTP2Action,
    GraphQLHTTP2Result
)

from hedra.core.engines.types.grpc import (
    GRPCAction,
    GRPCResult
)

from hedra.core.engines.types.http import (
    HTTPAction,
    HTTPResult
)

from hedra.core.engines.types.http2 import (
    HTTP2Action,
    HTTP2Result
)

from hedra.core.engines.types.http3 import (
    HTTP3Action,
    HTTP3Result
)

from hedra.core.engines.types.playwright import (
    PlaywrightCommand,
    PlaywrightResult
)

from hedra.core.engines.types.task import (
    Task,
    TaskResult
)

from hedra.core.engines.types.udp import (
    UDPAction,
    UDPResult
)

from hedra.core.engines.types.websocket import (
    WebsocketAction,
    WebsocketResult
)

from .cassandra_schema_set import CassandraSchemaSet


class CassandraConverter:

    def __init__(self) -> None:
        self.schemas = CassandraSchemaSet()