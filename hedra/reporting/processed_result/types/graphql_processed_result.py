from typing import Any, Dict, Union
from hedra.core.engines.types.graphql import GraphQLResult
from .http_processed_result import HTTPProcessedResult


class GraphQLProcessedResult(HTTPProcessedResult):
    
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
        result: GraphQLResult
    ) -> None:
        super(GraphQLProcessedResult, self).__init__(
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