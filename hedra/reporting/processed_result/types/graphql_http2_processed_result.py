from typing import Dict, Any, Union
from .http2_processed_result import HTTP2ProcessedResult
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Result


class GraphQLHTTP2ProcessedResult(HTTP2ProcessedResult):

    __slots__ = (
        'event_id',
        'action_id',
        'url',
        'ip_addr',
        'method',
        'path',
        'params',
        'hostname',
        'status',
        'headers',
        'data',
        'status',
        'timings',
        'query'
    )

    def __init__(
        self, 
        stage: str, 
        result: GraphQLHTTP2Result
    ) -> None:
        super(GraphQLHTTP2ProcessedResult, self).__init__(
            stage,
            result
        )

        self.query = result.query

    def to_dict(self) -> Dict[str, Union[str, int, float]]:
        graphql_result_dict = super().to_dict()

        return {
            **graphql_result_dict,
            'query': self.query
        }