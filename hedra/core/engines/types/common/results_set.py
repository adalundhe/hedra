from collections import defaultdict
from typing import Dict, List, Union, Any
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.graphql import GraphQLResult
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Result
from hedra.core.engines.types.grpc import GRPCResult
from hedra.core.engines.types.http import HTTPResult
from hedra.core.engines.types.http2 import HTTP2Result
from hedra.core.engines.types.playwright import PlaywrightResult
from hedra.core.engines.types.task import TaskResult
from hedra.core.engines.types.udp import UDPResult
from hedra.core.engines.types.websocket import WebsocketResult
from hedra.reporting.processed_result.results import results_types


ResultsBatch = Dict[str, Union[List[BaseResult], float]]


class ResultsSet:

    def __init__(
        self,
        execution_results: Dict[str, Union[int, float, List[ResultsBatch]]]
    ) -> None:

        
        self.total_elapsed: float = execution_results.get('total_elapsed', 0)
        self.total_results: int = execution_results.get('total_results', 0)


        self.results: List[BaseResult] = execution_results.get('stage_results', [])

        self.serialized_results: List[Dict[str, Any]] = execution_results.get('serialized_results', [])

        self.types = {
            RequestTypes.GRAPHQL: GraphQLResult,
            RequestTypes.GRAPHQL_HTTP2: GraphQLHTTP2Result,
            RequestTypes.GRPC: GRPCResult,
            RequestTypes.HTTP: HTTPResult,
            RequestTypes.HTTP2: HTTP2Result,
            RequestTypes.PLAYWRIGHT: PlaywrightResult,
            RequestTypes.TASK: TaskResult,
            RequestTypes.UDP: UDPResult,
            RequestTypes.WEBSOCKET: WebsocketResult
        }

    def __iter__(self):
        for result in self.results:
            yield result


    def to_serializable(self):
        return {
            'total_elapsed': self.total_elapsed,
            'total_results': self.total_results,
            'results': [result.to_dict() for result in self.results]
        }
    
    def load_results(self):
        self.results = [
            self.types.get(
                result.get('type'), 
                HTTPResult
            ).from_dict(result) for result in self.serialized_results
        ]

    def group(self) -> Dict[str, List[BaseResult]]:
        grouped_results = defaultdict(list)
        for result in self.results:
            grouped_results[result.name].append(result)

        return grouped_results