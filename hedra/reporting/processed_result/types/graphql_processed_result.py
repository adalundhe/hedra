from typing import Any, Tuple, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.graphql import GraphQLResult
from .http_processed_result import HTTPProcessedResult


class GraphQLProcessedResult(HTTPProcessedResult):

    def __init__(
        self, 
        stage: Any, 
        result: GraphQLResult
    ) -> None:
        super(GraphQLProcessedResult, self).__init__(
            stage,
            result
        )