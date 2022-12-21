import dill
import time
import statistics
import asyncio
from collections import defaultdict
from typing import Union, List, Dict, Union
from hedra.plugins.types.plugin_types import PluginType
from hedra.reporting.events import EventsGroup
from hedra.reporting.metric import MetricsSet
from hedra.core.graphs.events import Event
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook, 
    ContextHook,
    MetricHook,
)
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.reporting.events import results_types
from hedra.reporting.events.types import (
    HTTPEvent, 
    HTTP2Event, 
    GraphQLEvent, 
    GRPCEvent, 
    WebsocketEvent, 
    PlaywrightEvent,
    UDPEvent
)
from hedra.core.graphs.stages.base.stage import Stage
from .parallel import process_results_batch


Events = Union[HTTPEvent, HTTP2Event, GraphQLEvent, GRPCEvent, WebsocketEvent, PlaywrightEvent, UDPEvent]


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        self.raw_results = {}

        self.accepted_hook_types = [ HookType.METRIC, HookType.EVENT, HookType.CONTEXT ]
        self.requires_shutdown = True
        self.allow_parallel = True
        self.analysis_execution_time = 0

    @Internal()
    async def run(self):

        events: List[Union[EventHook, Event]] = [event for event in self.hooks[HookType.EVENT]]
        pre_events: List[EventHook] = [
            event for event in events if isinstance(event, EventHook) and event.pre
        ]
        
        if len(pre_events) > 0:
            pre_event_names = ", ".join([
                event.shortname for event in pre_events
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing PRE events - {pre_event_names}')
            await asyncio.wait([
                asyncio.create_task(event.call()) for event in pre_events
            ], timeout=self.stage_timeout)

        await self.logger.filesystem.aio.create_logfile('hedra.reporting.log')
        self.logger.filesystem.create_filelogger('hedra.reporting.log')
        
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting results analysis')

        analysis_execution_time_start = time.monotonic()

        engine_plugins = self.plugins_by_type.get(PluginType.ENGINE)
        for plugin_name, plugin in engine_plugins.items():
            results_types[plugin_name] = plugin.event

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Generated custom Event - {plugin.event.type} - for Reporter plugin - {plugin_name}')
        
        all_results = list(self.raw_results.items())
        summaries = {
            'session_total': 0,
            'stages': {}
        }

        metric_hook_names: List[str] = [hook.name for hook in self.hooks[HookType.METRIC]]

        batches = self.executor.partion_stage_batches(all_results)
        total_group_results = 0

        elapsed_times = []
        for stage_name, _, _ in batches:
            stage_results = self.raw_results.get(stage_name)
            total_group_results += stage_results.get('total_results', 0)
            elapsed_times.append(
                stage_results.get('total_elapsed', 0)
            )

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Paritioned {len(batches)} batches of results')

        median_execution_time = round(statistics.median(elapsed_times))
        await self.logger.spinner.append_message(f'Calculating stats for - {total_group_results} - actions executed over a median stage execution time of {median_execution_time} seconds')

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Calculating stats for - {total_group_results} - actions over a median stage execution time of {median_execution_time} seconds')

        custom_metric_hooks: List[MetricHook] = []
        for metric_hook_name in metric_hook_names:
            custom_metric_hook = registrar.all.get(metric_hook_name)
            custom_metric_hooks.append(custom_metric_hook)

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Loaded custom Metric hook - {metric_hook_name}')

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
                    'graph_name': self.graph_name,
                    'graph_id': self.graph_id,
                    'source_stage_name': self.name,
                    'source_stage_id': self.stage_id,
                    'analyze_stage_name': stage_name,
                    'analyze_stage_metric_hooks': list(metric_hook_names),
                    'analyze_stage_batched_results': batch
                })

            stage_configs.append((
                stage_name,
                assigned_workers_count,
                batch_configs
            ))

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Assigned {assigned_workers_count} to process results from stage - {stage_name}')

        stages_count = len(stage_configs)
        await self.logger.spinner.append_message(
            f'Calculating results for - {stages_count} - stages'
        )

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Processing results or - {stages_count} - stages')


        stage_batches = await self.executor.execute_batches(
            stage_configs,
            process_results_batch
        )

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Completed parital results aggregation for - {stages_count} - stages')

        processed_results = []

        self.logger.spinner.set_message_at(2, f'Converting aggregate results to metrics for - {stages_count} - stages.')

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Starting stage results aggregation for {stages_count} stages')
        
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

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Convererted stats for stage - {stage_name} to metrics set')
        
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Calculated results for - {events_group.total} - actions from stage - {stage_name}')

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

        self.analysis_execution_time = round(
            time.monotonic() - analysis_execution_time_start
        )

        post_events: List[EventHook] = [
            event for event in events if isinstance(event, EventHook) and event.pre is False
        ]

        if len(post_events) > 0:
            post_event_names = ", ".join([
                event.shortname for event in post_events
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing POST events - {post_event_names}')
            await asyncio.wait([
                asyncio.create_task(event.call()) for event in post_events
            ], timeout=self.stage_timeout)

        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        await asyncio.gather(*[
            asyncio.create_task(context_hook.call(self.context)) for context_hook in context_hooks
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed results analysis for - {stages_count} - stages in - {self.analysis_execution_time} seconds')
        await self.logger.spinner.set_default_message(f'Completed results analysis for {total_group_results} actions and {stages_count} stages over {self.analysis_execution_time} seconds')

        return summaries
