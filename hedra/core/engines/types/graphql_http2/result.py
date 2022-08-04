from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http2.result import HTTP2Result
from .action import GraphQLHTTP2Action


class GraphQLHTTP2Result(HTTP2Result):

    def __init__(self, action: GraphQLHTTP2Action, error: Exception = None) -> None:
        super().__init__(action, error)
        self.type = RequestTypes.GRAPHQL_HTTP2