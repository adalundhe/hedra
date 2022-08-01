import asyncio
from concurrent.futures import ThreadPoolExecutor
import time
import numpy
import inspect
from collections import defaultdict
from typing import Dict, List, Union
from easy_logger import Logger
import psutil
from hedra.reporting.metric import MetricsGroup
from hedra.core.hooks.types.hook import Hook
from hedra.core.hooks.types.types import HookType
from hedra.core.hooks.registry.registrar import registar
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
        self.hooks: Dict[str, List[Hook]] = {}
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

        self.timings = defaultdict(list)

        for hook_type in HookType:
            self.hooks[hook_type] = []

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()
        
    async def run(self):

        summaries = {
            'session_total': 0,
            'stages': {}
        }
        start = time.time()

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registar.all.get(method_name)

            if hook:
                self.hooks[hook.hook_type].append(hook)
        
        for stage_name, stage_results in self.raw_results.items():
    
            events =  defaultdict(EventsGroup)
            stage_total = 0
            stage_total_time = stage_results.get('total_elapsed')

            for stage_result in stage_results.get('results'):
                await events[stage_result.name].add(
                    stage_result,
                    stage_name
                )

            grouped_stats = {}
            
            for event_group_name, events_group in events.items():  
    
                group_total = events_group.succeeded + events_group.failed

                timings_dict = {}
                for group_name, group_timings in events_group.timings.items():
                    timings_group = self.calculate_timings_group(
                        group_name,
                        group_timings
                    )

                    timings_dict= {
                        **timings_dict,
                        **timings_group
                    }

                metric_data = {
                    'total': group_total,
                    'succeeded': events_group.succeeded,
                    'failed': events_group.failed,
                    'errors': list([
                        {
                            'message': error_message,
                            'count': error_count
                        } for error_message, error_count in events_group.errors.items()
                    ]),
                    'timings': timings_dict
                }

                metric = MetricsGroup(
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
                'aps': stage_total/stage_total_time,
                'actions': grouped_stats
            }

            print(stage_total/stage_total_time)

            summaries['session_total'] += stage_total

        stop = time.time()
        print('TOOK: ', stop-start)
        return summaries

    def calculate_timings_group(self, group_name: str, timings: List[Union[int, float]]):
        custom_metrics = {}
        for custom_metric in self.hooks.get(HookType.METRIC):
            result = custom_metric.call(timings)

            custom_metrics[custom_metric.shortname] = {
                'result': result,
                'field_type': custom_metric.reporter_field
            }

        quantiles = {
            f'quantile_{int(quantile_range * 100)}th': quantile for quantile, quantile_range in zip(
                numpy.quantile(timings, self.quantile_ranges),
                self.quantile_ranges
            )
        }
        
        return {
            group_name: {
                'median': float((numpy.median(timings))),
                'mean': float(numpy.mean(timings)),
                'variance': float(numpy.var(timings)),
                'stdev': float(numpy.std(timings)),
                'minimum': min(timings),
                'maximum': max(timings),
                'quantiles': quantiles,
                'custom_metrics': custom_metrics
            }

        }


    def calculate(self, results_group: List[Events]) -> Dict[str, Union[float, int, bool, str]]:
        return {}
            