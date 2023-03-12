from typing import Any
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