import asyncio
from collections import defaultdict
import time
import numpy
from typing import Dict, List, Union
from easy_logger import Logger
from hedra.reporting.metric import Metric
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.reporting.events.types import (
    HTTPEvent, 
    HTTP2Event, 
    GraphQLEvent, 
    GRPCEvent, 
    WebsocketEvent, 
    PlaywrightEvent
)
from hedra.reporting.events import results_types
from .stage import Stage


Events = Union[HTTPEvent, HTTP2Event, GraphQLEvent, GRPCEvent, WebsocketEvent, PlaywrightEvent]


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
    
            events =  defaultdict(list)
            stage_total = 0
            
            for stage_result in stage_results:
                event = results_types.get(stage_result.type)(stage_result)
                event.stage = stage_name
                events[event.shortname].append(event)
            
            grouped_stats = {}
            
            for event_group_name, events_group in events.items():  
                tags = {}
                succeeded = 0
                errors = defaultdict(lambda: 0)
                timings = []

                await asyncio.gather(*[event.check_result() for event in events_group])

                for event in events_group:

                    tags.update(event.tags_to_dict())

                    if event.error is None:
                        succeeded += 1
                    
                    else:
                        errors[event.error] += 1
                    
                    timings.append(event.time)

                failed = sum(errors.values())
                group_total = succeeded + failed

                quantile_ranges = [ .10, .20, .25, .30, .40, .50, .60, .70, .75, .80, .90, .95, .99 ]
                quantiles = {
                    quantile_range: quantile for quantile, quantile_range in zip(
                        numpy.quantile(timings, quantile_ranges),
                        quantile_ranges
                    )
                }

                custom_results = self.calculate(events_group)

                grouped_stats[event_group_name] = Metric(
                    event_group_name,
                    events_group[0].source,
                    stage_name,
                    {
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
                    },
                    tags
                )

                stage_total += group_total

            summaries[stage_name] = {
                'total': stage_total,
                **grouped_stats
            }

        stop = time.time()
        print('TOOK: ', stop-start)
        return summaries

    def calculate(self, results_group: List[Events]) -> Dict[str, Union[float, int, bool, str]]:
        return {}
            