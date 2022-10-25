import dill
from collections import defaultdict
from typing import Union, List, Dict
from easy_logger import Logger
from hedra.reporting.events import EventsGroup
from hedra.reporting.metric import MetricsSet
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.reporting.events.types import (
    HTTPEvent, 
    HTTP2Event, 
    GraphQLEvent, 
    GRPCEvent, 
    WebsocketEvent, 
    PlaywrightEvent,
    UDPEvent
)
from .parallel.analyze_results import group_batched_results
from .stage import Stage


Events = Union[HTTPEvent, HTTP2Event, GraphQLEvent, GRPCEvent, WebsocketEvent, PlaywrightEvent, UDPEvent]


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.raw_results = {}

        self.accepted_hook_types = [ HookType.METRIC ]
        self.requires_shutdown = True
        self.allow_parallel = True

    @Internal
    async def run(self):

        all_results = list(self.raw_results.items())
        summaries = {
            'session_total': 0,
            'stages': {}
        }

        metric_hook_names = [hook.name for hook in self.hooks.get(HookType.METRIC)]

        batches = self.executor.partion_stage_batches(all_results)

        custom_metric_hooks = []
        for metric_hook_name in metric_hook_names:
            custom_metric_hook = registrar.all.get(metric_hook_name)
            custom_metric_hooks.append(custom_metric_hook)

        stage_configs = []
        stage_total_times = {}

        for stage_name, _, assigned_workers_count in batches:
            
            stage_batches = []

            stage_results = self.raw_results.get(stage_name)
            results = stage_results.get('results')
            stage_total_time = stage_results.get('total_elapsed')
            
            stage_total_times[stage_name] = stage_total_time

            results_count = len(results)
            
            batch_size = int(results_count/assigned_workers_count)
            
            for worker_idx in range(assigned_workers_count):

                batch_marker = worker_idx * batch_size

                stage_batches.append(
                    results[batch_marker:batch_marker + batch_size]
                )

            if results_count%assigned_workers_count > 0:
                stage_batches[assigned_workers_count-1].extend(
                    results[assigned_workers_count * batch_size:]
                )

            batch_configs = []
            for batch in stage_batches:
                batch_configs.append({
                    'analyze_stage_name': stage_name,
                    'analyze_stage_metric_hooks': list(metric_hook_names),
                    'analyze_stage_batched_results': batch
                })

            stage_configs.append((
                stage_name,
                assigned_workers_count,
                batch_configs
            ))

        stage_batches = await self.executor.execute_batches(
            stage_configs,
            group_batched_results
        )

        processed_results = []
        
        for stage_name, stage_results in stage_batches:
        
            stage_total = 0
            stage_total_time = stage_total_times.get(stage_name)

            batch_results: List[Dict[str, Union[dict, EventsGroup]]] = [
                dill.loads(group) for group in stage_results
            ]

            events =  defaultdict(EventsGroup)

            for events_groups in batch_results:
                for event_group_name, events_group in events_groups.items():
                    events[event_group_name].merge(events_group)

            grouped_stats = {}
            
            for event_group_name, events_group in events.items():  

                custom_metrics = defaultdict(dict)
                for custom_metric in custom_metric_hooks:
                    custom_metrics[custom_metric.group][custom_metric.shortname] = await custom_metric.call(
                        self.raw_results.get(
                            stage_name
                        ).get('results')
                    )

                events_group.calculate_quantiles()

                metric_data = {
                    'total': events_group.total,
                    'succeeded': events_group.succeeded,
                    'failed': events_group.failed,
                    'actions_per_second': events_group.total/stage_total_time,
                    'errors': list([
                        {
                            'message': error_message,
                            'count': error_count
                        } for error_message, error_count in events_group.errors.items()
                    ]),
                    'groups': events_group.groups,
                    'custom': custom_metrics
                }

                metric = MetricsSet(
                    event_group_name,
                    events_group.source,
                    stage_name,
                    metric_data,
                    events_group.tags
                )

                grouped_stats[event_group_name] = metric

                stage_total += events_group.total

            processed_results.append({
                'stage_metrics': {
                    stage_name: {
                        'total': stage_total,
                        'actions_per_second': stage_total/stage_total_time,
                        'actions': grouped_stats
                    }
                },
                'stage_total': stage_total
            })

        for result in processed_results:
            summaries['stages'].update(result.get('stage_metrics'))
            summaries['session_total'] += result.get('stage_total')
         
        return summaries
