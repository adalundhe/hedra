from typing import Any
from hedra.core.engines.types.graphql import GraphQLResult
from .http_processed_result import HTTPProcessedResult


class HTTP3ProcessedResult(HTTPProcessedResult):

    def __init__(
        self, 
        stage: str, 
        result: GraphQLResult
    ) -> None:
        super(HTTP3ProcessedResult, self).__init__(
            stage,
            result
        )