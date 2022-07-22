import asyncio
from collections import defaultdict
import statistics
import time
import numpy
from typing import Dict, List, TypeVar, Union
from easy_logger import Logger
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.results.types import (
    HTTPResult, 
    HTTP2Result, 
    GraphQLResult, 
    GRPCResult, 
    WebsocketResult, 
    PlaywrightResult
)
from hedra.core.results import results_types
from .stage import Stage


Result = Union[HTTPResult, HTTP2Result, GraphQLResult, GRPCResult, WebsocketResult, PlaywrightResult]


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.raw_results = {}
        
    async def run(self):

        summaries = {}
        start = time.time()
        for stage_name, stage_results in self.raw_results.items():
    
            results =  defaultdict(list)
            stage_total = 0
            
            for stage_result in stage_results:
                result = results_types.get(stage_result.type)(stage_result)
                results[result.name].append(result)
            
            grouped_stats = {}
            
            for results_group_name, results_group in results.items():
                
                succeeded = 0
                errors = defaultdict(lambda: 0)
                timings = []

                await asyncio.gather(*[result.check_result() for result in results_group])

                for result in results_group:

                    if result.error is None:
                        succeeded += 1
                    
                    else:
                        errors[result.error] += 1
                    
                    timings.append(result.time)

                failed = sum(errors.values())
                group_total = succeeded + failed

                quantile_ranges = [ .10, .20, .25, .30, .40, .50, .60, .70, .75, .80, .90, .95, .99 ]
                quantiles = {
                    quantile_range: quantile for quantile, quantile_range in zip(
                        numpy.quantile(timings, quantile_ranges),
                        quantile_ranges
                    )
                }

                custom_results = self.calculate(results_group)

                grouped_stats[results_group_name] = {
                    'total': group_total,
                    'succeeded': succeeded,
                    'failed': failed,
                    'errors': list([
                        {
                            'message': error_message,
                            'count': error_count
                        } for error_message, error_count in errors.items()
                    ]),
                    'median': numpy.median(timings),
                    'mean': numpy.mean(timings),
                    'variance': numpy.var(timings),
                    'stdev': numpy.std(timings),
                    'minimum': min(timings),
                    'maximum': max(timings),
                    'quantiles': quantiles,
                    **custom_results
                }

                stage_total += group_total

            summaries[stage_name] = {
                'total': stage_total,
                **grouped_stats
            }

        stop = time.time()
        print('TOOK: ', stop-start)
        return summaries

    def calculate(self, results_group: List[Result]) -> Dict[str, Union[float, int, bool, str]]:
        return {}
            