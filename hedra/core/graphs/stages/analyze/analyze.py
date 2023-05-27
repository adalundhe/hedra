

import asyncio
import dill
import functools
import statistics
import signal
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Union, List, Dict, Any, Tuple
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.hooks.types.condition.decorator import condition
from hedra.core.hooks.types.context.decorator import context
from hedra.core.hooks.types.event.decorator import event  
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.event_types import EventType
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from hedra.logging import logging_manager
from hedra.monitoring import (
    CPUMonitor,
    MemoryMonitor
)
from hedra.plugins.types.plugin_types import PluginType
from hedra.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.metric.stage_metrics_summary import StageMetricsSummary
from hedra.reporting.metric.custom_metric import CustomMetric
from hedra.reporting.processed_result import results_types, ProcessedResultsGroup
from hedra.reporting.processed_result.types import (
    GraphQLProcessedResult,
    GraphQLHTTP2ProcessedResult,
    GRPCProcessedResult,
    HTTPProcessedResult,
    HTTP2ProcessedResult,
    PlaywrightProcessedResult,
    TaskProcessedResult,
    UDPProcessedResult,
    WebsocketProcessedResult,
)
from hedra.reporting.system import SystemMetricsSet
from hedra.reporting.system.system_metrics_set_types import MonitorGroup
from hedra.versioning.flags.types.base.active import active_flags
from hedra.versioning.flags.types.base.flag_type import FlagTypes
from typing import Optional
from .parallel import process_results_batch

dill.settings['byref'] = True


Events = Union[
    GraphQLProcessedResult, 
    GraphQLHTTP2ProcessedResult, 
    GRPCProcessedResult, 
    HTTPProcessedResult, 
    HTTP2ProcessedResult, 
    PlaywrightProcessedResult, 
    TaskProcessedResult,
    UDPProcessedResult,
    WebsocketProcessedResult
]

StageConfig = Tuple[str, int, List[Dict[str, List[BaseResult]]]]

RawResultsSet = Dict[str, ResultsSet]

RawResultsPairs = List[Tuple[Dict[str,  List[Tuple[str, Any]]]]]

ProcessedResults = Dict[str, Union[Dict[str, Union[int, float, int]], int]]

ProcessedMetricsSet = Dict[str, MetricsSet]

ProcessedStageMetricsSet = Dict[str, Union[int, float, Dict[str, ProcessedMetricsSet]]]

ProcessedResultsSet = List[Tuple[str, StageMetricsSummary]]

CustomMetricsSet = Dict[str, Dict[str, Dict[str, Union[int, float, Any]]]]

EventsSet = Dict[str, Dict[str, ProcessedResultsGroup]]

MonitorResults = Dict[str, List[Union[int, float]]]


def handle_loop_stop(
    signame,
    loop: asyncio.AbstractEventLoop,
    executor: ThreadPoolExecutor
):
    try:
        executor.shutdown(wait=False, cancel_futures=True)
        loop.close()

    except BrokenPipeError:
        pass

    except RuntimeError:
        pass


def deserialize_results(results: List[bytes]) -> List[BaseResult]:
    return [
        dill.loads(result) for result in results
    ]


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel=False
    handler=None
    priority: Optional[str]=None
    retries: int = 0

    def __init__(self) -> None:
        super().__init__()

        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.CONDITION,
            HookType.EVENT, 
            HookType.METRIC,
            HookType.TRANSFORM, 
        ]

        self.stage_retries = self.retries
        self.requires_shutdown = True
        self.allow_parallel = True
        self.analysis_execution_time = 0
        self.analysis_execution_time_start = 0
        self._executor = ThreadPoolExecutor(max_workers=self.workers)
        self._loop: asyncio.AbstractEventLoop = None
        self.source_internal_events = [
            'initialize_raw_results'
        ]

        self.internal_events = [
            'initialize_results_analysis',
            'partition_results_batches',
            'get_custom_metric_hooks',
            'create_stage_batches',
            'assign_stage_batches',
            'analyze_stage_batches',
            'reduce_stage_contexts',
            'merge_events_groups',
            'calculate_custom_metrics',
            'generate_metrics_sets',
            'generate_summary',
            'complete'
        ]

        self.priority = self.priority
        if self.priority is None:
            self.priority = 'auto'

        self.priority_level: StagePriority = StagePriority.map(
            self.priority
        )

    @Internal()
    async def run(self):  
        self.executor.batch_by_stages = True

        await self.setup_events()
        self.dispatcher.assemble_execution_graph()
        await self.dispatcher.dispatch_events(self.name)

    @context()
    async def initialize_results_analysis(
        self,
        analyze_stage_raw_results: RawResultsSet={},
        session_stage_monitors: MonitorGroup={},
        session_setup_stage_configs: Dict[str, Config]={}
    ):

        await self.logger.filesystem.aio.create_logfile('hedra.reporting.log')
        self.logger.filesystem.create_filelogger('hedra.reporting.log')
        
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting results analysis')

        self.analysis_execution_time_start = time.monotonic()

        engine_plugins = self.plugins_by_type.get(PluginType.ENGINE)
        for plugin_name, plugin in engine_plugins.items():
            results_types[plugin_name] = plugin.event

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Generated custom Event - {plugin.event.type} - for Reporter plugin - {plugin_name}')

        self.context.ignore_serialization_filters = [
            'analyze_stage_all_results',
            'analyze_stage_raw_results',
            'analyze_stage_target_stages',
            'analyze_stage_deserialized_results',
            'session_stage_monitors',
            'analyze_stage_monitors'
        ]

        all_results = list(analyze_stage_raw_results.items())

        total_group_results = 0
        for stage_results in analyze_stage_raw_results.values():
            total_group_results += stage_results.total_results
        
        return {
            'analyze_stage_raw_results': analyze_stage_raw_results,
            'analyze_stage_all_results': all_results,
            'analyze_stage_stages_count': len(analyze_stage_raw_results),
            'analyze_stage_total_group_results': total_group_results,
            'session_setup_stage_configs': session_setup_stage_configs,
            'session_stage_monitors': session_stage_monitors
        }
    
    @event('initialize_results_analysis')
    async def add_shutdown_handler(self):
        self._loop = asyncio.get_running_loop()

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._loop,
                    self.executor
                )
            )

    @condition('add_shutdown_handler')
    async def check_if_has_multiple_workers(self):
        return {
            'analyze_stage_has_multiple_workers': self.total_pool_cpus > 1
        }

    @event('check_if_has_multiple_workers')
    async def generate_deserialized_results(
        self,
        analyze_stage_raw_results: RawResultsSet={},
        analyze_stage_has_multiple_workers: bool=False
    ):
        deserialized_results = dict(analyze_stage_raw_results)
        cpu_monitor = CPUMonitor()
        memory_monitor = MemoryMonitor()

        main_monitor_name = f'{self.name}.main'

        await cpu_monitor.start_background_monitor(main_monitor_name)
        await memory_monitor.start_background_monitor(main_monitor_name)

        if analyze_stage_has_multiple_workers:
            for results_set_name, results_set in analyze_stage_raw_results.items():
                
                results_set_copy = results_set.copy()
                results_set_copy.results = await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        deserialize_results,
                        results_set.results
                    )
                )

                deserialized_results[results_set_name] = results_set_copy

        return {
            'analyze_stage_monitors': {
                'cpu': cpu_monitor,
                'memory': memory_monitor
            },
            'analyze_stage_raw_results': analyze_stage_raw_results,
            'analyze_stage_deserialized_results': deserialized_results
        }    


    @event('generate_deserialized_results')
    async def partition_results_batches(
        self,
        analyze_stage_raw_results: RawResultsSet={},
        analyze_stage_all_results: List[RawResultsPairs]=[]
    ):
        batches = self.executor.partion_stage_batches(analyze_stage_all_results)

        elapsed_times = []
        for stage_name, _, _ in batches:
            stage_results: ResultsSet = analyze_stage_raw_results.get(stage_name)
            elapsed_times.append(
                stage_results.total_elapsed
            )

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Paritioned {len(batches)} batches of results')

        return {
            'analyze_stage_batches': batches,
            'analyze_stage_elapsed_times': elapsed_times
        }
    
    @event('partition_results_batches')
    async def create_stage_batches(
        self,
        analyze_stage_raw_results: RawResultsSet=[],
        analyze_stage_batches: List[Tuple[str, Any, int]]=[],
        session_setup_stage_configs: Dict[str, Config]={}
    ):
        stage_total_times = {}
        stage_batch_sizes = {}
        stage_streamed_analytics: Dict[str, List[StreamAnalytics]] = {}
        analyze_stage_batch_configs = {}
        stage_personas = {}

        for stage_name, _, assigned_workers_count in analyze_stage_batches:
            
            stage_batches: List[List[Any]] = []

            stage_results = analyze_stage_raw_results.get(stage_name)
            results = stage_results.results
            stage_total_time = stage_results.total_elapsed
            
            stage_total_times[stage_name] = stage_total_time
            stage_streamed_analytics[stage_name] = stage_results.stage_streamed_analytics

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

            analyze_stage_batch_configs[stage_name] = stage_batches
            stage_batch_sizes[stage_name] = stage_results.stage_batch_size
            stage_personas[stage_name] = stage_results.stage_persona_type

        return {
            'analyze_stage_target_stages': {},
            'analyze_stage_batch_configs': analyze_stage_batch_configs,
            'analyze_stage_total_times': stage_total_times,
            'analyze_stage_batch_sizes': stage_batch_sizes,
            'analyze_stage_personas': stage_personas,
            'analyze_stage_streamed_analytics': stage_streamed_analytics
        }

    @context('create_stage_batches')
    async def assign_stage_batches(
        self,
        analyze_stage_batches: List[Tuple[str, Any, int]]=[],
        analyze_stage_batch_configs: Dict[str, List[List[Any]]]=[],
        analyze_stage_metric_hook_names: List[str]=[],
        analyze_stage_has_multiple_workers: bool=False
    ):
        if analyze_stage_has_multiple_workers:
            stage_configs = []
            serializable_context = self.context.as_serializable()
            worker_idx = 0

            for stage_name, _, assigned_workers_count in analyze_stage_batches:

                stage_configs.append((
                    stage_name,
                    assigned_workers_count,
                    [
                        {
                            'graph_name': self.graph_name,
                            'graph_path': self.graph_path,
                            'graph_id': self.graph_id,
                            'enable_unstable_features': active_flags[FlagTypes.UNSTABLE_FEATURE],
                            'logfiles_directory': logging_manager.logfiles_directory,
                            'log_level': logging_manager.log_level_name,
                            'source_stage_name': self.name,
                            'source_stage_context': {
                                context_key: context_value for context_key, context_value in serializable_context
                            },
                            'source_stage_id': self.stage_id,
                            'analyze_stage_name': stage_name,
                            'analyze_stage_metric_hooks': list(analyze_stage_metric_hook_names),
                            'analyze_stage_batched_results': batch,
                            'worker_id': worker_idx + 1
                        } for batch in analyze_stage_batch_configs[stage_name]
                    ]
                ))
                
                worker_idx += 1
                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Assigned {assigned_workers_count} to process results from stage - {stage_name}')

            return {
                'analyze_stage_configs': stage_configs,
            }
    
    @condition('assign_stage_batches')
    async def check_if_multiple_stages(
        self,
        analyze_stage_configs: List[Tuple[str, Any, int]]=[],
    ):
       return {
           'multiple_stages_to_process': len(analyze_stage_configs) > 1
       } 

    @event('check_if_multiple_stages')
    async def execute_batched_analysis(
        self,
        analyze_stage_stages_count: int=0,
        analyze_stage_elapsed_times: List[float]=[],
        analyze_stage_total_group_results: int=0,
        analyze_stage_configs: List[StageConfig]=[],
        multiple_stages_to_process: bool=False,
        analyze_stage_deserialized_results: RawResultsSet = {},
        analyze_stage_has_multiple_workers: bool=False,
    ):

        if multiple_stages_to_process and analyze_stage_has_multiple_workers:
            await self.logger.spinner.append_message(
                f'Calculating results for - {analyze_stage_stages_count} - stages'
            )

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Processing results or - {analyze_stage_stages_count} - stages')
            
            median_execution_time = round(statistics.median(analyze_stage_elapsed_times))
            await self.logger.spinner.append_message(f'Calculating stats for - {analyze_stage_total_group_results} - actions executed over a median stage execution time of {median_execution_time} seconds')

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Calculating stats for - {analyze_stage_total_group_results} - actions over a median stage execution time of {median_execution_time} seconds')
            stage_batch_results = await self.executor.execute_batches(
                analyze_stage_configs,
                process_results_batch
            )

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Completed parital results aggregation for - {analyze_stage_stages_count} - stages')

            return {
                'analyze_stage_batch_results': stage_batch_results
            }
        
        else:
            

            batch_results = []
            events =  defaultdict(ProcessedResultsGroup)

            for results_set in analyze_stage_deserialized_results.values():

                for stage_result in results_set.results:
                    events[stage_result.name].add(
                            results_set.stage,
                            stage_result,
                        )
                    
                batch_results.append(
                    (results_set.stage, events)
                )
                
                for events_group in events.values():
                    events_group.calculate_stats()
                    
            return {
                'analyze_stage_batch_results': batch_results
            }
        
    @event('execute_batched_analysis')
    async def merge_events_groups(
        self,
        analyze_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]]={},
        analyze_stage_batch_results: List[Tuple[str, List[Dict[str, Any]]]]=[],
        multiple_stages_to_process: bool=False
    ):

        stage_events_set = {}

        stage_cpu_monitor: CPUMonitor = analyze_stage_monitors.get('cpu')
        stage_memory_monitor: MemoryMonitor = analyze_stage_monitors.get('memory')

        if multiple_stages_to_process:

            stage_cpu_monitor.stage_type = StageTypes.ANALYZE
            stage_memory_monitor.stage_type = StageTypes.ANALYZE

            for stage_name, stage_results in analyze_stage_batch_results:
                results = stage_results.pop()

                worker_id = results.get('worker_id')
                stage_events_set[stage_name] = results.get('events')

                monitors: Dict[str, MonitorResults] = results.get('monitoring', {})
                cpu_monitor = monitors.get('cpu', {})
                memory_monitor = monitors.get('memory', {})
                
                for monitor_name, collection_stats in cpu_monitor.items():
                    stage_cpu_monitor.worker_metrics[worker_id][monitor_name] = collection_stats
                    stage_cpu_monitor.collected[monitor_name].extend(collection_stats)

                for monitor_name, collection_stats in memory_monitor.items():
                    stage_memory_monitor.worker_metrics[worker_id][monitor_name] = collection_stats

            stage_cpu_monitor.aggregate_worker_stats()
            stage_memory_monitor.aggregate_worker_stats()

        else:

            for stage_name, stage_results in analyze_stage_batch_results:
                stage_events_set[stage_name] = stage_results

        main_monitor_name = f'{self.name}.main'

        await stage_cpu_monitor.stop_background_monitor(main_monitor_name)
        await stage_memory_monitor.stop_background_monitor(main_monitor_name)

        stage_cpu_monitor.stage_metrics[main_monitor_name] = stage_cpu_monitor.collected[main_monitor_name]
        stage_memory_monitor.stage_metrics[main_monitor_name] = stage_memory_monitor.collected[main_monitor_name]

        stage_cpu_monitor.close()
        stage_memory_monitor.close()

        return {
            'analyze_stage_events_set': stage_events_set,
            'analyze_stage_monitors': {
                'cpu': stage_cpu_monitor,
                'memory': stage_memory_monitor
            }
        }

    @event('merge_events_groups')
    async def calculate_custom_metrics(self):

        custom_metrics_set = defaultdict(dict)

        metrics = [
            metric_event for metric_event in self.dispatcher.events[EventType.METRIC]
        ]

        for metric in metrics:
            for context_key, context_value in metric.context:
                if isinstance(context_value, CustomMetric):
                    custom_metrics_set[context_value.metric_group][context_key] = context_value

        return {
            'analyze_stage_custom_metrics_set': custom_metrics_set
        }

    @event('calculate_custom_metrics')
    async def generate_metrics_sets(
        self,
        analyze_stage_custom_metrics_set: CustomMetricsSet={},
        analyze_stage_events_set: EventsSet={},
        analyze_stage_total_times: Dict[str, float]={},
        analyze_stage_personas: Dict[str, str]={},
        analyze_stage_batch_sizes: Dict[str, int]={},
        analyze_stage_streamed_analytics: Dict[str, List[StreamAnalytics]] = {}
    ):

        processed_results = []
        
        for stage_name, stage_events in analyze_stage_events_set.items():    

            stage_total_time = analyze_stage_total_times.get(stage_name)

            persona_type = analyze_stage_personas.get(stage_name)
            batch_size = analyze_stage_batch_sizes.get(stage_name)

            stage_metrics_summary = StageMetricsSummary(
                stage_name=stage_name,
                persona_type=persona_type,
                batch_size=batch_size,
                total_elapsed=stage_total_time,
                stage_streamed_analytics=analyze_stage_streamed_analytics.get(stage_name)
            )
            grouped_stats = {}

            for event_group_name, events_group in stage_events.items():  

                custom_metrics = analyze_stage_custom_metrics_set.get(event_group_name, {})
                
                events_group.calculate_quantiles()

                metric_data = {
                    'total': events_group.total,
                    'succeeded': events_group.succeeded,
                    'failed': events_group.failed,
                    'actions_per_second': round(
                        events_group.total/stage_total_time,
                        2
                    ),
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

                stage_metrics_summary.metrics_sets[event_group_name] = metric

                grouped_stats[event_group_name] = metric

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Convererted stats for stage - {stage_name} to metrics set')
        
            stage_metrics_summary.calculate_action_and_task_metrics()
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Calculated results for - {stage_metrics_summary.stage_metrics.total} - actions from stage - {stage_name}')

            processed_results.append((
                stage_name,
                stage_metrics_summary
            ))

        return {
            'analyze_stage_processed_results': processed_results
        }
    
    @event('generate_metrics_sets')
    async def generate_experiment_metrics(
        self,
        analyze_stage_processed_results: ProcessedResultsSet=[],
        analyze_stage_raw_results: RawResultsSet=[],
    ):
        
        experiment_metrics_sets: Dict[str, ExperimentMetricsSet] = {}

        for stage_name, stage_metrics in analyze_stage_processed_results:

            
            stage_metrics_sets = stage_metrics.metrics_sets

            raw_results_set = analyze_stage_raw_results.get(stage_name)
            experiment = raw_results_set.experiment
            
            if experiment:
                variant = experiment.get('experiment_variant')
                variant_name = variant.get('variant_stage')
                mutations = variant.get('variant_mutations')

                experiment_name = experiment.get('experiment_name')


                experiment_metrics_set = experiment_metrics_sets.get(experiment_name)
                if experiment_metrics_set is None:
                    experiment_metrics_set = ExperimentMetricsSet()

                if experiment_metrics_set.experiment_name is None:
                    experiment_metrics_set.experiment_name = experiment.get('experiment_name')

                
                if experiment_metrics_set.randomized is None:
                    experiment_metrics_set.randomized = experiment.get('experiment_randomized')

                experiment_metrics_set.participants.append(stage_name)

                variant['stage_batch_size'] = raw_results_set.stage_batch_size
                variant['stage_optimized'] = raw_results_set.stage_optimized
                variant['stage_persona_type'] = raw_results_set.stage_persona_type
                variant['stage_workers'] = raw_results_set.stage_workers

                experiment_metrics_set.variants[variant_name] = variant
                experiment_metrics_set.mutations[variant_name] = mutations
                experiment_metrics_set.metrics[variant_name] = stage_metrics_sets

                experiment_metrics_sets[experiment_name] = experiment_metrics_set

        for experiment_metrics_set in experiment_metrics_sets.values():
            experiment_metrics_set.generate_experiment_summary()

        return {
            'experiment_metrics_sets': experiment_metrics_sets
        }
    
    @event('generate_metrics_sets')
    async def generate_system_metrics(
        self,
        analyze_stage_monitors: MonitorGroup={},
        session_stage_monitors: MonitorGroup={},
        analyze_stage_batch_sizes: Dict[str, int]={},
    ):
        
        session_stage_monitors.update({
            self.name: {
                **analyze_stage_monitors
            }
        })

        system_metrics_set = SystemMetricsSet(
            session_stage_monitors,
            analyze_stage_batch_sizes
        )
        system_metrics_set.generate_system_summaries()

        return {
            'analyze_stage_system_metrics': system_metrics_set
        }

    @context(
        'generate_experiment_metrics',
        'generate_system_metrics'
    )
    async def generate_summary(
        self,
        analyze_stage_stages_count: int=0,
        analyze_stage_total_group_results: int=0,
        analyze_stage_processed_results: ProcessedResultsSet=[],
        analyze_stage_contexts: Dict[str, Any]={},
        experiment_metrics_sets: Dict[str, ExperimentMetricsSet]={},
        analyze_stage_system_metrics: SystemMetricsSet=None,
        analyze_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]] = {}
    ):

        self.context[self.name] = analyze_stage_contexts
        
        summaries: Dict[
            str, 
            Union[
                str,
                Dict[str, StageMetricsSummary], 
                Dict[str, MetricsSet], 
                SystemMetricsSet
            ]
        ] = {
            'stages': {},
            'source': self.name,
            'experiment_metrics_sets': experiment_metrics_sets,
            'system_metrics': analyze_stage_system_metrics
        }

        for stage_name, stage_metrics in analyze_stage_processed_results:
            summaries['stages'][stage_name] = stage_metrics

        self.analysis_execution_time = round(
            time.monotonic() - self.analysis_execution_time_start
        )

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed results analysis for - {analyze_stage_stages_count} - stages in - {self.analysis_execution_time} seconds')
        await self.logger.spinner.set_default_message(f'Completed results analysis for {analyze_stage_total_group_results} actions and {analyze_stage_stages_count} stages over {self.analysis_execution_time} seconds')

        self.executor.shutdown()
        
        return {
            'analyze_stage_summary_metrics': summaries,
            'analyze_stage_monitors': {
                self.name: {
                    **analyze_stage_monitors
                }
            }
        }

    @event('generate_summary')
    async def complete(self):
        return {}
