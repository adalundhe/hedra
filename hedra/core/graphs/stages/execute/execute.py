
import dill
import time
import statistics
from collections import defaultdict
from typing import (
    Generic, 
    List, 
    Union, 
    Any, 
    Dict, 
    Optional,
    Type
)
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.client import Client
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.engines.types.playwright import (
    MercuryPlaywrightClient,
    ContextConfig
)
from hedra.core.engines.types.registry import registered_engines
from hedra.core.hooks.types.condition.decorator import condition
from hedra.core.hooks.types.context.decorator import context
from hedra.core.hooks.types.event.decorator import event
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.partition_method import PartitionMethod
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from hedra.core.personas.persona_registry import (
    get_persona, 
    registered_personas, 
    DefaultPersona
)
from hedra.data.serializers import Serializer
from hedra.logging import logging_manager
from hedra.monitoring import (
    CPUMonitor,
    MemoryMonitor
)
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.extension.types import ExtensionType
from hedra.plugins.types.extension.extension_plugin import ExtensionPlugin
from hedra.reporting.reporter import ReporterConfig
from hedra.versioning.flags.types.base.active import active_flags
from hedra.versioning.flags.types.base.flag_type import FlagTypes
from hedra.plugins.extensions import get_enabled_extensions
from .parallel import execute_actions


MonitorResults = Dict[str, List[Union[int, float]]]


T = TypeVarTuple('T')


class Execute(Stage, Generic[Unpack[T]]):
    stage_type=StageTypes.EXECUTE
    priority: Optional[str]=None
    retries: int=0

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client: Client[Unpack[T]] = Client(
            self.graph_name,
            self.graph_id,
            self.name,
            self.stage_id
        )
        
        self.accepted_hook_types = [ 
            HookType.ACTION,
            HookType.CHANNEL, 
            HookType.CHECK,
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT,
            HookType.TASK,
            HookType.TRANSFORM
        ]

        self.concurrent_pool_aware_stages = 0
        self.execution_stage_id = 0
        self.optimized = False
        self.execute_setup_stage = None
        self.requires_shutdown = True
        self.allow_parallel = True

        self.source_internal_events = [
            'get_stage_config'
        ]

        self.internal_events = [
            'get_stage_config',
            'get_stage_plugins',
            'check_has_multiple_workers',
            'run_multiple_worker_jobs',
            'aggregate_multiple_worker_results',
            'setup_single_worker_job',
            'run_single_worker_job',
            'complete'
        ]

        self.priority = self.priority
        if self.priority is None:
            self.priority = 'auto'

        self.priority_level: StagePriority = StagePriority.map(
            self.priority
        )

        self.stage_retries = self.retries
        self.serializer = Serializer()

    @Internal()
    async def run(self):
        await self.setup_events()
        self.dispatcher.assemble_execution_graph()
        await self.dispatcher.dispatch_events(self.name)

    @context()
    async def get_stage_config(
        self,
        execute_stage_stream_configs: List[ReporterConfig] = [],
        execute_stage_setup_config: Config=None,
        execute_stage_setup_by: str=None,
        execute_stage_setup_hooks: List[Union[ActionHook , TaskHook]] = []
    ):
        self.context.ignore_serialization_filters = [
            'execute_stage_setup_hooks',
            'setup_stage_ready_stages',
            'execute_stage_results',
            'execute_stage_streamed_analytics',
            'execute_stage_monitors',
            'session_stage_monitors',
            'execute_stage_loaded_actions'
        ]
        persona_type_name = '-'.join([
            segment.capitalize() for segment in execute_stage_setup_config.persona_type.split('-')
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing - {execute_stage_setup_config.batch_size} - VUs over {self.workers} threads for {execute_stage_setup_config.total_time_string} using - {persona_type_name} - persona')
        await self.logger.spinner.append_message(f'Stage {self.name} executing - {execute_stage_setup_config.batch_size} - VUs over {self.workers} threads for {execute_stage_setup_config.total_time_string} using - {persona_type_name} - persona')

        main_monitor_name = f'{self.name}.main'

        cpu_monitor = CPUMonitor()
        memory_monitor = MemoryMonitor()

        cpu_monitor.visibility_filters[main_monitor_name] = False
        memory_monitor.visibility_filters[main_monitor_name] = False

        cpu_monitor.stage_type = StageTypes.EXECUTE
        memory_monitor.stage_type = StageTypes.EXECUTE
        cpu_monitor.is_execute_stage = True
        memory_monitor.is_execute_stage = True

        await cpu_monitor.start_background_monitor(main_monitor_name)
        await memory_monitor.start_background_monitor(main_monitor_name)

        return {
            'execute_stage_stream_configs': execute_stage_stream_configs,
            'execute_stage_setup_hooks': execute_stage_setup_hooks,
            'execute_stage_setup_by': execute_stage_setup_by,
            'execute_stage_setup_config': execute_stage_setup_config,
            'execute_stage_monitors': {
                'cpu': cpu_monitor,
                'memory': memory_monitor
            }
        }
    
    @event('get_stage_config')
    async def collect_loaded_actions(self):

        loaded_actions: List[Union[ActionHook, TaskHook]] = []

        ignore_context_keys = set()

        for context_key, value in self.context.items():

            if context_key != 'execute_stage_setup_hooks':

                if isinstance(value, ActionHook):
                    ignore_context_keys.add(context_key)
                    loaded_actions.append(value)

                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, ActionHook):
                            ignore_context_keys.add(context_key)
                            loaded_actions.append(item)

                elif isinstance(value, dict):
                    for item in value.values():
                        if isinstance(item, ActionHook):
                            ignore_context_keys.add(context_key)
                            loaded_actions.append(item)

        
        self.context.ignore_serialization_filters.extend(
            list(ignore_context_keys)
        )

        return {
            'execute_stage_loaded_actions': loaded_actions
        }

    @event('get_stage_config')
    async def get_stage_plugins(self):

        engine_plugins = self.plugins_by_type.get(PluginType.ENGINE)
        for plugin_name, plugin in engine_plugins.items():
            registered_engines[plugin_name] = plugin
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Engine plugin - {plugin.name}')

        persona_plugins = self.plugins_by_type.get(PluginType.PERSONA)
        for plugin_name, plugin in persona_plugins.items():
            registered_personas[plugin_name] = lambda plugin_config: plugin(plugin_config)
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Persona plugin - {plugin.name}')
        
        execute_stage_extensions: Dict[str, ExtensionPlugin] = {}

        stage_extension_plugins: List[str] = self.plugins_by_type.get(
            PluginType.EXTENSION, {}
        )

        extension_plugins: Dict[str, Type[ExtensionPlugin]] = get_enabled_extensions()
        for plugin_name in stage_extension_plugins:
            plugin = extension_plugins.get(plugin_name)

            if plugin:
                enabled_plugin = plugin()
                execute_stage_extensions[enabled_plugin.name] = enabled_plugin

        source_stage_plugins = defaultdict(list)
        for plugin in self.plugins.values():
            source_stage_plugins[plugin.type].append(plugin.name)

        return {
            'execute_stage_plugins': source_stage_plugins,
            'execute_stage_extensions': execute_stage_extensions
        }
    
    @event('get_stage_config')
    async def get_stage_experiment(
        self,
        execute_stage_setup_config: Config=None,
    ):

        experiment = execute_stage_setup_config.experiment

        if experiment:

            mutations = []

            if execute_stage_setup_config.mutations:
                mutations = [
                    {
                        'mutation_name': mutation.name,
                        'mutation_chance': mutation.chance,
                        'mutation_targets': mutation.targets,
                        'mutation_type': mutation.mutation_type.name.lower()
                    } for mutation in execute_stage_setup_config.mutations
                ]


            return {
                'execute_stage_experiment': {
                    'experiment_name': experiment.get('experiment_name'),
                    'experiment_randomized': experiment.get('random'),
                    'experiment_variant': {
                        'variant_stage': self.name,
                        'variant_weight': experiment.get('weight'),
                        'variant_distribution_intervals': experiment.get('intervals'),
                        'variant_distribution': experiment.get('distribution'),
                        'variant_distribution_type': experiment.get('distribution_type'),
                        'variant_distribution_interval_duration': experiment.get('interval_duration'),
                        'variant_mutations': mutations
                    }
                }
            }

    @condition(
        'collect_loaded_actions',
        'get_stage_plugins',
        'get_stage_experiment'
    )
    async def check_has_multiple_workers(self):
        return {
            'execute_stage_has_multiple_workers': self.total_pool_cpus > 1
        }

    @event('check_has_multiple_workers')
    async def run_multiple_worker_jobs(
        self,
        execute_stage_loaded_actions: List[ActionHook]=[],
        execute_stage_has_multiple_workers: bool = False,
        execute_stage_setup_config: Config=None,
        execute_stage_plugins: Dict[str, List[Any]]={},
        execute_stage_setup_by: str=None,
        execute_stage_stream_configs: List[ReporterConfig] = []
    ):
        loaded_actions: List[str] = []
        for action_hook in execute_stage_loaded_actions:
            loaded_actions.append(
                self.serializer.serialize_action(action_hook)
            ) 

        if execute_stage_has_multiple_workers:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting execution for - {self.workers} workers')

            serializable_context = self.context.as_serializable() 
            results_sets = await self.executor.execute_stage_batch(
                execute_actions,
                [
                    dill.dumps({
                        'graph_name': self.graph_name,
                        'graph_path': self.graph_path,
                        'graph_id': self.graph_id,
                        'enable_unstable_features': active_flags[FlagTypes.UNSTABLE_FEATURE],
                        'source_stage_name': self.name,
                        'logfiles_directory': logging_manager.logfiles_directory,
                        'log_level': logging_manager.log_level_name,
                        'source_stage_context': {
                            context_key: context_value for context_key, context_value in serializable_context
                        },
                        'source_stage_loaded_actions': loaded_actions,
                        'source_setup_stage_name': execute_stage_setup_by,
                        'source_stage_id': self.stage_id,
                        'source_stage_plugins': execute_stage_plugins,
                        'source_stage_config': execute_stage_setup_config,
                        'source_stage_stream_configs': execute_stage_stream_configs,
                        'partition_method': PartitionMethod.BATCHES,
                        'workers': self.workers,
                        'worker_id': idx + 1
                    }) for idx in range(self.workers)
                ]
            )

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed execution for - {self.workers} workers')    


            return {
                'execute_stage_results': results_sets
            }

    @context('run_multiple_worker_jobs')
    async def aggregate_multiple_worker_results(
        self,
        execute_stage_has_multiple_workers: bool = False,
        execute_stage_results: List[Dict[str, Any]]=[],
        execute_stage_experiment: Optional[Dict[str, Any]]=None,
        execute_stage_setup_config: Config=None,
        execute_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]]={}
    ):
        if execute_stage_has_multiple_workers:
            execute_stage_streamed_analytics: List[StreamAnalytics] = []
            aggregate_results = []
            elapsed_times = []
            stage_contexts = defaultdict(list)

            stage_cpu_monitor: CPUMonitor = execute_stage_monitors.get('cpu')
            stage_memory_monitor: MemoryMonitor = execute_stage_monitors.get('memory')
            
            for result_set in execute_stage_results:

                aggregate_results.extend(result_set.get('results'))
                elapsed_times.append(result_set.get('total_elapsed'))
                worker_id = result_set.get('worker_id')
                
                streamed_analytics = result_set.get('streamed_analytics')
                if streamed_analytics:
                    execute_stage_streamed_analytics.append(streamed_analytics)

                pipeline_context: Dict[str, Any] = result_set.get('context', {})
                for context_key, context_value in pipeline_context.items():
                    stage_contexts[context_key].append(context_value)

                monitors: Dict[str, MonitorResults] = result_set.get('monitoring', {})
                cpu_monitor = monitors.get('cpu', {})
                memory_monitor = monitors.get('memory', {})
                
                for monitor_name, collection_stats in cpu_monitor.items():
                    stage_cpu_monitor.worker_metrics[worker_id][monitor_name] = collection_stats
                    stage_cpu_monitor.collected[monitor_name].extend(collection_stats)

                for monitor_name, collection_stats in memory_monitor.items():
                    stage_memory_monitor.worker_metrics[worker_id][monitor_name] = collection_stats

            for monitor_name, collection_stats in cpu_monitor.items():
                stage_cpu_monitor.visibility_filters[monitor_name] = True
                
            for monitor_name, collection_stats in memory_monitor.items():
                stage_memory_monitor.visibility_filters[monitor_name] = True

            stage_cpu_monitor.aggregate_worker_stats()
            stage_memory_monitor.aggregate_worker_stats()

            main_monitor_name = f'{self.name}.main'

            await stage_cpu_monitor.stop_background_monitor(main_monitor_name)
            await stage_memory_monitor.stop_background_monitor(main_monitor_name)

            stage_cpu_monitor.close()
            stage_memory_monitor.close()

            stage_cpu_monitor.stage_metrics[main_monitor_name] = stage_cpu_monitor.collected[main_monitor_name]
            stage_memory_monitor.stage_metrics[main_monitor_name] = stage_memory_monitor.collected[main_monitor_name]

            total_results = len(aggregate_results)
            total_elapsed = statistics.mean(elapsed_times)

            await self.logger.filesystem.aio['hedra.core'].info( f'{self.metadata_string} - Completed - {total_results} actions at  {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')
            await self.logger.spinner.set_default_message(f'Stage - {self.name} completed {total_results} actions at {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')

            stage_name = self.name.lower()

            return {
                stage_name: stage_contexts,
                'execute_stage_streamed_analytics': execute_stage_streamed_analytics,
                'execute_stage_results':  ResultsSet({
                    'stage': self.name,
                    'streamed_analytics': execute_stage_streamed_analytics,
                    'stage_batch_size': execute_stage_setup_config.batch_size,
                    'stage_persona_type': execute_stage_setup_config.persona_type,
                    'stage_workers': self.workers,
                    'stage_optimized': self.optimized,
                    'stage_results': aggregate_results,
                    'total_results': total_results,
                    'total_elapsed': total_elapsed,
                    'experiment': execute_stage_experiment
                }),
                'execute_stage_monitors': {
                    self.name: {
                        'cpu': stage_cpu_monitor,
                        'memory': stage_memory_monitor
                    }
                }
            }

    @event('check_has_multiple_workers')
    async def setup_single_worker_job(
        self,
        execute_stage_loaded_actions: List[ActionHook]=[],
        execute_stage_has_multiple_workers: bool = False,
        execute_stage_setup_config: Config=None,
        execute_stage_extensions: Dict[str, ExtensionPlugin]={}
    ):


        self.hooks[HookType.ACTION].extend(execute_stage_loaded_actions)

        if execute_stage_has_multiple_workers is False:

            for extension in execute_stage_extensions.values():

                if extension.extension_type == ExtensionType.GENERATOR:
                    results = await extension.execute(**{
                        'execute_stage_name': self.name,
                        'execute_stage_hooks': self.hooks,
                        'persona_config': execute_stage_setup_config
                    })

                    execute_stage = results.get('execute_stage')

                    if execute_stage:
                        self.hooks = results.get('execute_stage_hooks')

            persona_config = execute_stage_setup_config
            persona = get_persona(persona_config)
            persona.setup(self.hooks, self.metadata_string)

            action_and_task_hooks: List[Union[ActionHook, TaskHook]] = [
                *self.hooks[HookType.ACTION],
                *self.hooks[HookType.TASK]
            ]

            for hook in action_and_task_hooks:
                if hook.action.type == RequestTypes.PLAYWRIGHT and isinstance(hook.session, MercuryPlaywrightClient):

                    await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Setting up Playwright Session')

                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Browser Type: {persona_config.browser_type}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Device Type: {persona_config.device_type}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Locale: {persona_config.locale}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - geolocation: {persona_config.geolocation}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Permissions: {persona_config.permissions}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Color Scheme: {persona_config.color_scheme}')


                    await hook.session.setup(
                        config=ContextConfig(
                            browser_type=persona_config.browser_type,
                            device_type=persona_config.device_type,
                            locale=persona_config.locale,
                            geolocation=persona_config.geolocation,
                            permissions=persona_config.permissions,
                            color_scheme=persona_config.color_scheme,
                            options=persona_config.playwright_options
                        )
                    )

            return {
                'execute_stage_persona': persona
            }


    @context('setup_single_worker_job')
    async def run_single_worker_job(
        self,
        execute_stage_has_multiple_workers: bool = False,
        execute_stage_persona: DefaultPersona=None,
        execute_stage_experiment: Optional[Dict[str, Any]]=None,
        execute_stage_setup_config: Config=None,
        execute_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]]={}
    ):
        if execute_stage_has_multiple_workers is False:

            execution_results = {}

            stage_cpu_monitor: CPUMonitor = execute_stage_monitors.get('cpu')
            stage_memory_monitor: MemoryMonitor = execute_stage_monitors.get('memory')

            main_monitor_name = f'{self.name}.main'

            start = time.monotonic()

            results = await execute_stage_persona.execute()

            elapsed = time.monotonic() - start

            await stage_cpu_monitor.stop_background_monitor(main_monitor_name)
            await stage_memory_monitor.stop_background_monitor(main_monitor_name)

            stage_cpu_monitor.close()
            stage_memory_monitor.close()

            elapsed_idx = int(elapsed)

            stage_cpu_monitor.trim_monitor_samples(
                main_monitor_name,
                elapsed_idx
            )

            stage_memory_monitor.trim_monitor_samples(
                main_monitor_name,
                elapsed_idx
            )

            await self.logger.filesystem.aio['hedra.core'].info(
                f'{self.metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds'
            )  

            total_results = len(results)
            total_elapsed = execute_stage_persona.total_elapsed

            await stage_cpu_monitor.stop_background_monitor(main_monitor_name)
            await stage_memory_monitor.stop_background_monitor(main_monitor_name)

            stage_cpu_monitor.close()
            stage_memory_monitor.close()

            stage_cpu_monitor.collected.update(
                execute_stage_persona.cpu_monitor.collected
            )

            stage_memory_monitor.collected.update(
                execute_stage_persona.memory_monitor.collected
            )

            stage_cpu_monitor.stage_metrics[main_monitor_name] = stage_cpu_monitor.collected[main_monitor_name]
            stage_memory_monitor.stage_metrics[main_monitor_name] = stage_memory_monitor.collected[main_monitor_name]


            await self.logger.filesystem.aio['hedra.core'].info( f'{self.metadata_string} - Completed - {total_results} actions at  {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')
            await self.logger.spinner.set_default_message(f'Stage - {self.name} completed {total_results} actions at {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')

            if self.executor:
                await self.executor.shutdown()

            execution_results.update({
                'execute_stage_streamed_analytics': [
                    execute_stage_persona.streamed_analytics
                ],
                'execute_stage_results': ResultsSet({
                    'stage': self.name,
                    'streamed_analytics': [
                        execute_stage_persona.streamed_analytics
                    ],
                    'stage_batch_size': execute_stage_setup_config.batch_size,
                    'stage_persona_type': execute_stage_setup_config.persona_type,
                    'stage_workers': self.workers,
                    'stage_optimized': self.optimized,
                    'stage_results': results,
                    'total_results': total_results,
                    'total_elapsed': total_elapsed,
                    'experiment': execute_stage_experiment
                }),
                'execute_stage_monitors': {
                    self.name: {   
                        'cpu': stage_cpu_monitor,
                        'memory': stage_memory_monitor
                    }
                }
            })
            
            return execution_results

    @event('aggregate_multiple_worker_results', 'setup_single_worker_job')
    async def complete(self):
        return {}