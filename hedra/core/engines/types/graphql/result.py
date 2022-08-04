from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.result import HTTPResult
from .action import GraphQLAction


class GraphQLResult(HTTPResult):

    def __init__(self, action: GraphQLAction, error: Exception = None) -> None:
        super().__init__(action, error)

        self.type = RequestTypes.GRAPHQL