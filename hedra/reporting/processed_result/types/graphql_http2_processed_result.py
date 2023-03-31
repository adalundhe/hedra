from typing import Dict, Any, Union
from .http2_processed_result import HTTP2ProcessedResult
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Result


class GraphQLHTTP2ProcessedResult(HTTP2ProcessedResult):

    def __init__(
        self, 
        stage: Any, 
        result: GraphQLHTTP2Result
    ) -> None:
        super(GraphQLHTTP2ProcessedResult, self).__init__(
            stage,
            result
        )

    def to_dict(self) -> Dict[str, Union[str, int, float]]:
        graphql_result_dict = super().to_dict()

        return {
            **graphql_result_dict,
            'query': self.query
        }