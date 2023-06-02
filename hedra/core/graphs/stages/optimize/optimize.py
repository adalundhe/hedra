import asyncio
import dill
import psutil
import signal
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from hedra.core.engines.client.config import Config
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.playwright import (
    MercuryPlaywrightClient,
    ContextConfig
)
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.optimize.optimization.algorithms import registered_algorithms
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.condition.decorator import condition
from hedra.core.hooks.types.context.decorator import context
from hedra.core.hooks.types.event.decorator import event
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.graphs.stages.optimize.optimization import Optimizer, DistributionFitOptimizer
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from hedra.data.serializers import Serializer
from hedra.logging import logging_manager
from hedra.monitoring import (
    CPUMonitor,
    MemoryMonitor
)
from hedra.monitoring.base.exceptions import MonitorKilledError
from hedra.plugins.extensions import get_enabled_extensions
from hedra.plugins.types.extension.types import ExtensionType
from hedra.plugins.types.extension.extension_plugin import ExtensionPlugin
from hedra.plugins.types.plugin_types import PluginType
from hedra.versioning.flags.types.base.active import active_flags
from hedra.versioning.flags.types.base.flag_type import FlagTypes
from typing import (
    Dict, 
    List, 
    Tuple, 
    Any, 
    Union,
    Optional,
    Type
)
from .optimization.parameters import Parameter
from .parallel import optimize_stage


BatchedOptimzationCandidates = List[Tuple[str, Execute, int]] 

OptimizeParameterPair = Tuple[Union[int, float], Union[int, float]]

MonitorResults = Dict[str, List[Union[int, float]]]


def handle_loop_stop(
    signame,
    executor: ThreadPoolExecutor
):
    try:
        executor.shutdown(cancel_futures=True)

    except KeyboardInterrupt:
        raise RuntimeError()

    except BrokenPipeError:
        raise RuntimeError()
        
    except RuntimeError:
        raise RuntimeError()
    
    except MonitorKilledError:
        raise RuntimeError()



class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    optimize_iterations=0
    algorithm='shg'
    time_limit='1m'
    optimize_params: List[Parameter]=[
        Parameter(
            'batch_size',
            minimum=0.5,
            maximum=2,
            feed_forward=True
        )
    ]
    priority: Optional[str]=None
    retries: int=0
    
    def __init__(self) -> None:
        super().__init__()
        self.generation_optimization_candidates = 0
        self.execution_stage_id = 0

        self.results = None

        time_parser = TimeParser(self.time_limit)
        self.stage_time_limit = time_parser.time
        self.requires_shutdown = True
        self.allow_parallel = True

        self.optimization_execution_time_start = 0
        self.optimization_execution_time = 0
        self.accepted_hook_types = [ 
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.TRANSFORM 
        ]

        self.optimize_iterations = self.optimize_iterations
        self.algorithm = self.algorithm
        self.time_limit = self.time_limit
        self.optimize_params = self.optimize_params

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._thread_executor: Union[ThreadPoolExecutor, None] = None

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
    async def collect_optimization_stages(
        self,
        setup_stage_configs: Dict[str, Config] = {},
        optimize_stage_candidates: Dict[str, Execute]={},
        setup_stage_experiment_config: Dict[str, Union[str, int, List[float]]]={},
        execute_stage_streamed_analytics: Dict[str, List[StreamAnalytics]]={}

    ):
        main_monitor_name = f'{self.name}.main'

        cpu_monitor = CPUMonitor()
        memory_monitor = MemoryMonitor()

        cpu_monitor.stage_type = StageTypes.OPTIMIZE
        memory_monitor.stage_type = StageTypes.OPTIMIZE

        await cpu_monitor.start_background_monitor(main_monitor_name)
        await memory_monitor.start_background_monitor(main_monitor_name)

        self.optimization_execution_time_start = time.monotonic()

        self.context.ignore_serialization_filters = [
            'optimize_stage_workers_map',
            'optimize_stage_batched_stages',
            'optimize_stage_candidates',
            'optimize_stage_monitors',
            'setup_stage_ready_stages',
            'execute_stage_setup_hooks',
            'execute_stage_results',
            'session_stage_monitors'
        ]

        stage_names = ', '.join(list(optimize_stage_candidates.keys()))

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Optimizing stages {stage_names} using {self.algorithm} algorithm')
        await self.logger.spinner.append_message(f'Optimizer - {self.name} optimizing stages {stage_names} using {self.algorithm} algorithm')

        optimize_stages = [(
            stage.name, 
            stage
        ) for stage in optimize_stage_candidates.values()]


        stages_count = len(optimize_stage_candidates)

        # We may have less workers available during the optimize stage than assigned
        # to the execute stage, so store the original workers count for later.
        stage_workers_map = {
            stage.name: stage.workers for stage in optimize_stage_candidates.values()
        }

        batched_stages: BatchedOptimzationCandidates = list(self.executor.partion_stage_batches(optimize_stages))

        return {
            'setup_stage_experiment_config': setup_stage_experiment_config,
            'setup_stage_configs': setup_stage_configs,
            'execute_stage_streamed_analytics': execute_stage_streamed_analytics,
            'optimize_stage_candidates': optimize_stage_candidates,
            'optimize_stage_stage_names': stage_names,
            'optimize_stage_stages_count': stages_count,
            'optimize_stage_workers_map': stage_workers_map,
            'optimize_stage_batched_stages': batched_stages,
            'optimize_stage_monitors': {
                'cpu': cpu_monitor,
                'memory': memory_monitor
            }
        }
    
    @event()
    async def get_stage_plugins(self):

        optimize_plugins = self.plugins_by_type.get(PluginType.OPTIMIZER)
        for plugin_name, plugin in optimize_plugins.items():
            registered_algorithms[plugin_name] = lambda plugin_config: plugin(plugin_config)
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Optimizer plugin - {plugin_name}')
  
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
            'optimize_stage_plugins': source_stage_plugins,
            'optimize_stage_extensions': execute_stage_extensions
        }
    
    @event()
    async def collect_loaded_actions(
        self,
        optimize_stage_candidates: Dict[str, Execute]={},
    ):
        loaded_actions: Dict[str, List[ActionHook]] = defaultdict(list)

        for candidate_stage in optimize_stage_candidates.values():

            for value in candidate_stage.context.values():

                if isinstance(value, ActionHook):
                    loaded_actions[candidate_stage.name].append(value)

                elif isinstance(value, list):

                    for item in value:
                        if isinstance(item, ActionHook):
                            loaded_actions[candidate_stage.name].append(item)

                elif isinstance(value, dict):

                    for item in value.values():
                        if isinstance(item, ActionHook):
                            loaded_actions[candidate_stage.name].append(item)

        return {
            'optimize_stage_loaded_actions': loaded_actions
        }

    
    @condition(
        'collect_optimization_stages',
        'get_stage_plugins'
    )
    async def check_has_multiple_workers(self):
        return {
            'optimize_stage_has_multiple_workers': self.total_pool_cpus > 1
        }


    @event('check_has_multiple_workers')
    async def create_optimization_configs(
        self,
        setup_stage_configs: Dict[str, Config] = {},
        optimize_stage_stages_count: int=0,
        optimize_stage_batched_stages: BatchedOptimzationCandidates=[],
        setup_stage_experiment_config: Dict[str, Union[str, int, List[float]]]={},
        execute_stage_streamed_analytics: Dict[str, List[StreamAnalytics]]={},
        optimize_stage_loaded_actions: Dict[str, List[ActionHook]]=[],
        optimize_stage_has_multiple_workers: bool = False
    ):
        if optimize_stage_has_multiple_workers:

            batched_configs = []

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Batching optimization for - {optimize_stage_stages_count} stages')
            
            serializable_context = self.context.as_serializable()

            for stage_name, stage, assigned_workers_count in optimize_stage_batched_stages:

                configs = []
                selected_stage_config = setup_stage_configs.get(stage.name) 

                batch_size = int(selected_stage_config.batch_size/assigned_workers_count)

                stage_loaded_actions = optimize_stage_loaded_actions.get(stage_name)
                loaded_actions: List[str] = [
                    self.serializer.serialize_action(
                        action_hook
                    ) for action_hook in stage_loaded_actions
                ]

                for worker_id in range(assigned_workers_count):

                    execute_stage_plugins = defaultdict(list)

                    for plugin in stage.plugins.values():
                        execute_stage_plugins[plugin.type].append(plugin.name)


                    configs.append({
                        'graph_name': self.graph_name,
                        'graph_path': self.graph_path,
                        'graph_id': self.graph_id,
                        'enable_unstable_features': active_flags[FlagTypes.UNSTABLE_FEATURE],
                        'logfiles_directory': logging_manager.logfiles_directory,
                        'log_level': logging_manager.log_level_name,
                        'worker_id': worker_id,
                        'source_stage_context': {
                            context_key: context_value for context_key, context_value in serializable_context
                        },
                        'source_stage_name': self.name,
                        'source_stage_id': self.stage_id,
                        'execute_stage_name': stage_name,
                        'setup_stage_experiment_config': setup_stage_experiment_config,
                        'execute_stage_loaded_actions': loaded_actions,
                        'execute_stage_streamed_analytics': execute_stage_streamed_analytics,
                        'execute_stage_generation_count': assigned_workers_count,
                        'execute_stage_id': stage.execution_stage_id,
                        'execute_stage_config': selected_stage_config,
                        'execute_stage_batch_size': batch_size,
                        'execute_setup_stage_name': stage.context['execute_stage_setup_by'],
                        'execute_stage_plugins': execute_stage_plugins,
                        'optimizer_params': self.optimize_params,
                        'optimizer_iterations': self.optimize_iterations,
                        'optimizer_algorithm': self.algorithm,
                        'optimize_stage_workers': self.workers,
                        'time_limit': self.stage_time_limit
                    })

                configs[assigned_workers_count-1]['execute_stage_batch_size'] += batch_size%assigned_workers_count

                configs = [
                    dill.dumps(config) for config in configs
                ]

                batched_configs.append((
                    stage_name,
                    assigned_workers_count,
                    configs
                ))

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Provisioned - {assigned_workers_count} - workers for stage - {stage_name}')

            return {
                'optimize_stage_batched_configs': batched_configs
            }

    @event('create_optimization_configs')
    async def execute_batched_optimization(
        self,
        optimize_stage_stages_count: int=0,
        optimize_stage_batched_configs: Dict[str, Any]={},
        optimize_stage_has_multiple_workers: bool = False
        
    ):
        if optimize_stage_has_multiple_workers:

            optimization_results = []
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting optimizaiton for - {optimize_stage_stages_count} - stages')

            results = await self.executor.execute_batches(
                optimize_stage_batched_configs,
                optimize_stage
            )


            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Completed optimizaiton for - {optimize_stage_stages_count} - stages')

            for _, result_batch in results:
                optimization_results.extend([
                    dill.loads(results_set) for results_set in result_batch
                ])

            return {
                'optimize_stage_results': optimization_results
            }
        
    @event('check_has_multiple_workers')
    async def setup_single_worker_optimization(
        self,
        setup_stage_configs: Dict[str, Config]={},
        optimize_stage_extensions: Dict[str, ExtensionPlugin] = {},
        optimize_stage_has_multiple_workers: bool = False,
        optimize_stage_candidates: Dict[str, Execute]={},
        
    ):
        if optimize_stage_has_multiple_workers is False:

            self._loop = asyncio.get_event_loop()
            self._thread_executor = ThreadPoolExecutor(
                max_workers=psutil.cpu_count(logical=False)
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):

                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._thread_executor
                    )
                )

            for stage_name, persona_config in setup_stage_configs.items():

                execute_stage: Execute = optimize_stage_candidates.get(stage_name)

                for extension in optimize_stage_extensions.values():

                    if extension.extension_type == ExtensionType.GENERATOR:
                        results = await extension.execute(**{
                            'execute_stage_name': execute_stage.name,
                            'execute_stage_hooks': execute_stage.hooks,
                            'persona_config': persona_config
                        })

                        execute_stage = results.get('execute_stage')

                        if execute_stage:
                            execute_stage.hooks = results.get('execute_stage_hooks')

                action_and_task_hooks: List[Union[ActionHook, TaskHook]] = [
                    *execute_stage.hooks[HookType.ACTION],
                    *execute_stage.hooks[HookType.TASK]
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

                optimize_stage_candidates[stage_name] = execute_stage

            return {
                'optimize_stage_candidates': optimize_stage_candidates
            }

    @event('setup_single_worker_optimization')
    async def execute_single_worker_optimization(
        self,
        setup_stage_configs: Dict[str, Config]={},
        optimize_stage_candidates: Dict[str, Execute]={},
        optimize_stage_has_multiple_workers: bool = False,
        execute_stage_streamed_analytics: Dict[str, List[StreamAnalytics]]={}

    ):
        
        optimizers: List[Union[DistributionFitOptimizer, Optimizer]] = []

        if optimize_stage_has_multiple_workers is False:
            for stage_name, persona_config in setup_stage_configs.items():

                execute_stage: Execute = optimize_stage_candidates.get(stage_name)

                if persona_config.experiment and persona_config.experiment.get('distribution'):

                    optimizer = DistributionFitOptimizer({
                        'graph_name': self.graph_name,
                        'graph_id': self.graph_id,
                        'source_stage_name': self.name,
                        'source_stage_id': self.stage_id,
                        'params': self.optimize_params,
                        'stage_name': stage_name,
                        'stage_config': persona_config,
                        'stage_hooks': execute_stage.hooks,
                        'iterations': self.optimize_iterations,
                        'algorithm': self.algorithm,
                        'time_limit': self.time_limit,
                        'stream_analytics': execute_stage_streamed_analytics
                    })

                else:
                    optimizer = Optimizer({
                        'graph_name': self.graph_name,
                        'graph_id': self.graph_id,
                        'source_stage_name': self.name,
                        'source_stage_id': self.stage_id,
                        'params': self.optimize_params,
                        'stage_name': stage_name,
                        'stage_config': persona_config,
                        'stage_hooks': execute_stage.hooks,
                        'iterations': self.optimize_iterations,
                        'algorithm': self.algorithm,
                        'time_limit': self.time_limit,
                        'stream_analytics': execute_stage_streamed_analytics
                    })

                optimizers.append(optimizer)

            results: List[Dict[str, Union[int, str, float]]] = await asyncio.gather(*[
                self._loop.run_in_executor(
                    self._thread_executor,
                    optimizer.optimize
                ) for optimizer in optimizers
            ])

            self._thread_executor.shutdown(cancel_futures=True)

            optimization_results: List[Dict[str, Union[int, str, float]]] = []
            for optimization_result in results:

                target_stage_name = optimization_result.get('optimize_target_stage')
                stage_config = setup_stage_configs.get(target_stage_name)
                

                if stage_config.experiment and optimization_result.get('optimized_distribution'):
                    stage_config.experiment['distribution'] = optimization_result.get('optimized_distribution')

                stage_config.batch_size = optimization_result.get('optimized_batch_size', stage_config.batch_size)
                stage_config.batch_interval = optimization_result.get('optimized_batch_interval', stage_config.batch_interval)
                stage_config.batch_gradient = optimization_result.get('optimized_batch_gradient', stage_config.batch_gradient)

                await self.logger.filesystem.aio['hedra.optimize'].info(f'{optimizer.metadata_string} - Optimization complete')

                
                optimization_results.append({
                    'stage': target_stage_name,
                    'config': stage_config,
                    'params': results
                })

            return {
                'optimize_stage_results': optimization_results
            }

    @event('execute_batched_optimization')
    async def collect_optimized_batch_sizes(
        self,
        optimize_stage_results: List[Dict[str, Any]]=[],
        optimize_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]] = {},
        optimize_stage_has_multiple_workers: bool = False
    ):

        optimized_batch_sizes = []
        for optimization_result in optimize_stage_results:
            optimized_config: Config = optimization_result.get('config')
            optimized_batch_sizes.append(optimized_config.batch_size)

        optimized_batch_size = sum(optimized_batch_sizes)

        if optimize_stage_has_multiple_workers:
        
            stage_cpu_monitor: CPUMonitor = optimize_stage_monitors.get('cpu')
            stage_memory_monitor: MemoryMonitor = optimize_stage_monitors.get('memory')

            optimized_batch_sizes = []
            for optimization_result in optimize_stage_results:
                monitors: Dict[str, MonitorResults] = optimization_result.get('monitoring', {})
                worker_id = optimization_result.get('worker_id')

                cpu_monitor = monitors.get('cpu', {})
                memory_monitor = monitors.get('memory', {})
                
                for monitor_name, collection_stats in cpu_monitor.items():
                    stage_cpu_monitor.worker_metrics[worker_id][monitor_name] = collection_stats
                    stage_cpu_monitor.collected[monitor_name].extend(collection_stats)

                for monitor_name, collection_stats in memory_monitor.items():
                    stage_memory_monitor.worker_metrics[worker_id][monitor_name] = collection_stats

            stage_cpu_monitor.aggregate_worker_stats()
            stage_memory_monitor.aggregate_worker_stats()

        main_monitor_name = f'{self.name}.main'

        await stage_cpu_monitor.stop_background_monitor(main_monitor_name)
        await stage_memory_monitor.stop_background_monitor(main_monitor_name)

        stage_cpu_monitor.close()
        stage_memory_monitor.close()

        stage_cpu_monitor.stage_metrics[main_monitor_name] = stage_cpu_monitor.collected[main_monitor_name]
        stage_memory_monitor.stage_metrics[main_monitor_name] = stage_memory_monitor.collected[main_monitor_name]

        return {
            'optimize_stage_batch_size': optimized_batch_size,
            'optimize_stage_monitors': {
                'cpu': stage_cpu_monitor,
                'memory': stage_memory_monitor
            }
        }

    @context('collect_optimized_batch_sizes')
    async def set_optimized_configs(
        self,
        optimize_stage_results: List[Any]=[],
        optimize_stage_candidates: Dict[str, Execute]={},
        optimize_stage_batch_size: int=0
    ):
       
        stage_optimzations = {}
        stage_context = defaultdict(list)
        optimized_stages = {}
        optimzied_hooks = defaultdict(list)
        optimized_configs: Dict[str, Any] = {}
        stages_setup_by = {}

        for optimization_result in optimize_stage_results:
            
            stage_name = optimization_result.get('stage')
            optimized_config: Config = optimization_result.get('config')

            stage = optimize_stage_candidates.get(stage_name)
            optimized_configs[stage.name] = optimized_config
            stages_setup_by[stage.name] = stage.context['execute_stage_setup_by']

            for hook in stage.dispatcher.actions_and_tasks.values():
                if hook.source.session:
                    hook.source.session.pool.size = optimize_stage_batch_size
                    hook.source.session.sem = asyncio.Semaphore(optimize_stage_batch_size)
                    hook.source.session.pool.connections = []
                    hook.source.session.pool.create_pool()

                    optimzied_hooks[hook.source.stage].append(hook)

            stage.context['execute_stage_setup_hooks'] = list(stage.dispatcher.actions_and_tasks.values())
            
            pipeline_context = optimization_result.get('context', {})
            for context_key, context_value in pipeline_context.items():
                stage_context[context_key].append(context_value)

            stage_optimzations[stage_name] = optimize_stage_batch_size

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Stage - {stage_name} - configured to use optimized batch size of - {optimize_stage_batch_size} - VUs')

            optimized_config.optimized = True
            stage.optimized = True

            optimized_stages[stage.name] = stage

        return {
            'optimize_stage_optimized_hooks': optimzied_hooks,
            'optimize_stage_optimized_configs': optimized_configs,
            'execute_stage_setup_by': stages_setup_by,
            'optimize_stage_optimzations': stage_optimzations,
            'optimize_stage_context': stage_context
        }

    @context('set_optimized_configs')
    async def complete_optimization(
        self,
        optimize_stage_stage_names: str=None,
        optimize_stage_results: List[Any]=[],
        optimize_stage_workers_map: Dict[str, Execute]={},
        optimize_stage_candidates: Dict[str, Execute]={},
        optimize_stage_optimzations: Dict[str, Union[int, float]]={},
        optimize_stage_context: Dict[str, Any]={},
        optimize_stage_monitors: Dict[str, Union[CPUMonitor, MemoryMonitor]]={}
    ):
        self.context[self.name] = optimize_stage_context

        for stage in optimize_stage_candidates.values():
            stage.workers = optimize_stage_workers_map.get(stage.name)

        self.optimization_execution_time = round(time.monotonic() - self.optimization_execution_time_start)

        optimized_batch_sizes = ', '.join([
            f'{stage_name}: {optimized_batch_size}' for stage_name, optimized_batch_size in optimize_stage_optimzations.items()
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Optimization complete for stages - {optimize_stage_stage_names} - over - {self.optimization_execution_time} - seconds')
        await self.logger.spinner.set_default_message(f'Optimized - batch sizes for stages - {optimized_batch_sizes} - over {self.optimization_execution_time} seconds')
        
        return {
            'optimize_stage_optimized_params': [
                result.get('params') for result in optimize_stage_results
            ],
            'optimize_stage_monitors': {
                self.name: {
                    **optimize_stage_monitors
                }
            }
        }

    @event('complete_optimization')
    async def complete(self):
        await self.executor.shutdown()

