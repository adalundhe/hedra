import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
import numpy
import inspect
from collections import defaultdict
from typing import Dict, List, Union
from easy_logger import Logger
import psutil
from hedra.reporting.metric import MetricsSet
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.reporting.events.types import (
    HTTPEvent, 
    HTTP2Event, 
    GraphQLEvent, 
    GRPCEvent, 
    WebsocketEvent, 
    PlaywrightEvent
)
from hedra.reporting.events import EventsGroup
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
        self.quantile_ranges = [ 
            .10, 
            .20, 
            .25, 
            .30, 
            .40, 
            .50, 
            .60, 
            .70, 
            .75, 
            .80, 
            .90, 
            .95, 
            .99 
        ]

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()
        self.accepted_hook_types = [ HookType.METRIC ]

    @Internal
    async def run(self):

        summaries = {
            'session_total': 0,
            'stages': {}
        }
        start = time.time()
        
        for stage_name, stage_results in self.raw_results.items():
    
            events =  defaultdict(EventsGroup)
            stage_total = 0
            stage_total_time = stage_results.get('total_elapsed')

            tasks = []
            for stage_result in stage_results.get('results'):
                tasks.append(
                    asyncio.create_task(events[stage_result.name].add(
                        stage_result,
                        stage_name
                    ))
                )

            await asyncio.gather(*tasks)

            grouped_stats = {}
            
            for event_group_name, events_group in events.items():  

                custom_metrics = defaultdict(dict)
                for custom_metric in self.hooks.get(HookType.METRIC):
                    custom_metrics[custom_metric.group][custom_metric.shortname] = await custom_metric.call(events_group)
    
                group_total = events_group.succeeded + events_group.failed

                metrics_groups = {}
                for group_name, group_timings in events_group.timings.items():
                    metrics_group = self.calculate_group_stats(
                        group_name,
                        group_timings
                    )

                    metrics_groups = {**metrics_groups, **metrics_group}

                metric_data = {
                    'total': group_total,
                    'succeeded': events_group.succeeded,
                    'failed': events_group.failed,
                    'actions_per_second': group_total/stage_total_time,
                    'errors': list([
                        {
                            'message': error_message,
                            'count': error_count
                        } for error_message, error_count in events_group.errors.items()
                    ]),
                    'groups': metrics_groups,
                    'custom': custom_metrics
                }

                metric = MetricsSet(
                    event_group_name,
                    events_group.events[0].source,
                    stage_name,
                    metric_data,
                    events_group.tags
                )

                grouped_stats[event_group_name] = metric

                stage_total += group_total

            summaries['stages'][stage_name] = {
                'total': stage_total,
                'actions_per_second': stage_total/stage_total_time,
                'actions': grouped_stats
            }

            print(stage_total/stage_total_time)

            summaries['session_total'] += stage_total

        stop = time.time()
        print('TOOK: ', stop-start)
        return summaries

    def calculate_group_stats(self, group_name: str, data: List[Union[int, float]]):

        quantiles = {
            f'quantile_{int(quantile_range * 100)}th': quantile for quantile, quantile_range in zip(
                numpy.quantile(data, self.quantile_ranges),
                self.quantile_ranges
            )
        }
        
        return {
            group_name: {
                'median': float((numpy.median(data))),
                'mean': float(numpy.mean(data)),
                'variance': float(numpy.var(data)),
                'stdev': float(numpy.std(data)),
                'minimum': min(data),
                'maximum': max(data),
                'quantiles': quantiles
            }

        }


    def calculate(self, results_group: List[Events]) -> Dict[str, Union[float, int, bool, str]]:
        return {}
            