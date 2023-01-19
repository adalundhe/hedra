from typing import Dict, Union
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.result import HTTPResult
from .action import GraphQLAction


class GraphQLResult(HTTPResult):

    def __init__(self, action: GraphQLAction, error: Exception = None) -> None:
        super().__init__(action, error)

        self.type = RequestTypes.GRAPHQL

    @classmethod
    def from_dict(cls, results_dict: Dict[str, Union[int, float, str,]]):
        
        action = GraphQLAction(
            results_dict.get('name'),
            results_dict.get('url'),
            method=results_dict.get('method'),
            user=results_dict.get('user'),
            tags=results_dict.get('tags'),
        )

        response = GraphQLResult(action, error=results_dict.get('error'))
        

        response.headers.update(results_dict.get('headers', {}))
        response.data = results_dict.get('data')
        response.status = results_dict.get('status')
        response.reason = results_dict.get('reason')
        response.checks = results_dict.get('checks')
     
        response.wait_start = results_dict.get('wait_start')
        response.start = results_dict.get('start')
        response.connect_end = results_dict.get('connect_end')
        response.write_end = results_dict.get('write_end')
        response.complete = results_dict.get('complete')

        return response