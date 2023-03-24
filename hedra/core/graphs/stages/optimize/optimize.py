import asyncio
import dill
import time
import pickle
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Union
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.hooks.types.context.decorator import context
from hedra.core.hooks.types.event.decorator import event
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.stages.base.stage import Stage
from .parallel import optimize_stage


BatchedOptimzationCandidates = List[Tuple[str, Execute, int]] 


class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    optimize_iterations=0
    algorithm='shg'
    stage_time_limit='1m'
    optimize_params={
        'batch_size': (0.5, 2)
    }
    
    def __init__(self) -> None:
        super().__init__()
        self.generation_optimization_candidates = 0
        self.execution_stage_id = 0

        self.results = None

        time_parser = TimeParser(self.stage_time_limit)
        self.time_limit = time_parser.time
        self.requires_shutdown = True
        self.allow_parallel = True

        self.optimization_execution_time_start = 0
        self.optimization_execution_time = 0
        self.accepted_hook_types = [ 
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.TRANSFORM 
        ]

    @Internal()
    async def run(self):

        await self.setup_events()
        await self.dispatcher.dispatch_events(self.name)

    @context()
    async def collect_optimization_stages(
        self,
        optimize_stage_candidates: Dict[str, Execute]={}
    ):
        self.optimization_execution_time_start = time.monotonic()

        self.context.ignore_serialization_filters = [
            'optimize_stage_workers_map',
            'optimize_stage_batched_stages',
            'optimize_stage_candidates',
            'setup_stage_ready_stages',
            'execute_stage_setup_hooks',
            'execute_stage_results'
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
            'optimize_stage_candidates': optimize_stage_candidates,
            'optimize_stage_stage_names': stage_names,
            'optimize_stage_stages_count': stages_count,
            'optimize_stage_workers_map': stage_workers_map,
            'optimize_stage_batched_stages': batched_stages
        }

    @event('collect_optimization_stages')
    async def create_optimization_configs(
        self,
        optimize_stage_stages_count: int=0,
        optimize_stage_batched_stages: BatchedOptimzationCandidates=[],
    ):
        batched_configs = []

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Batching optimization for - {optimize_stage_stages_count} stages')
        
        serializable_context = self.context.as_serializable()

        for stage_name, stage, assigned_workers_count in optimize_stage_batched_stages:

            configs = []
            stage_config = stage.context['execute_stage_setup_config'] 

            batch_size = int(stage_config.batch_size/assigned_workers_count)

            for worker_idx in range(assigned_workers_count):

                execute_stage_plugins = defaultdict(list)

                for plugin in stage.plugins.values():
                    execute_stage_plugins[plugin.type].append(plugin.name)

                configs.append({
                    'graph_name': self.graph_name,
                    'graph_path': self.graph_path,
                    'graph_id': self.graph_id,
                    'optimize_params': self.optimize_params,
                    'worker_idx': worker_idx,
                    'source_stage_context': {
                        context_key: context_value for context_key, context_value in serializable_context
                    },
                    'source_stage_name': self.name,
                    'source_stage_id': self.stage_id,
                    'execute_stage_name': stage_name,
                    'execute_stage_generation_count': assigned_workers_count,
                    'execute_stage_id': stage.execution_stage_id,
                    'execute_stage_config': stage.context['execute_stage_setup_config'],
                    'execute_stage_batch_size': batch_size,
                    'execute_setup_stage_name': stage.context['execute_stage_setup_by'],
                    'execute_stage_plugins': execute_stage_plugins,
                    'optimizer_iterations': self.optimize_iterations,
                    'optimizer_algorithm': self.algorithm,
                    'time_limit': self.time_limit
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
    async def execute_optimization(
        self,
        optimize_stage_stages_count: int=0,
        optimize_stage_batched_configs: Dict[str, Any]={},
    ):
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

    @event('execute_optimization')
    async def collect_optimized_batch_sizes(
        self,
        optimize_stage_results: List[Any]=[],
    ):
        optimized_batch_sizes = []
        for optimization_result in optimize_stage_results:
            optimized_config = optimization_result.get('config')
            optimized_batch_sizes.append(
                optimized_config.batch_size
            )

        optimized_batch_size = sum(optimized_batch_sizes)

        return {
            'optimize_stage_batch_size': optimized_batch_size
        }

    @context('collect_optimized_batch_sizes')
    async def set_optimized_batch_size(
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
            optimized_config = optimization_result.get('config')

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

    @context('set_optimized_batch_size')
    async def complete_optimization(
        self,
        optimize_stage_stage_names: str=None,
        optimize_stage_results: List[Any]=[],
        optimize_stage_workers_map: Dict[str, Execute]={},
        optimize_stage_candidates: Dict[str, Execute]={},
        optimize_stage_optimzations: Dict[str, Union[int, float]]={},
        optimize_stage_context: Dict[str, Any]={}
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
            ]
        }

    @event('complete_optimization')
    async def complete(self):
        await self.executor.shutdown()

