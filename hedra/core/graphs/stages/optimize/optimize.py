import asyncio
import dill
import time
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Union
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    TaskHook
)
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.hooks.hook_types.context import context
from hedra.core.graphs.hooks.hook_types.event import event
from hedra.core.graphs.hooks.hook_types.internal import Internal
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
        await self.dispatcher.dispatch_events()

    @context()
    async def collect_optimization_stages(
        self,
        optimization_candidates: Dict[str, Execute]={}
    ):
        self.optimization_execution_time_start = time.monotonic()

        self.context.ignore_serialization_filters = [
            'optimization_candidates',
            'stage_workers_map',
            'batched_stages'
        ]

        stage_names = ', '.join(list(optimization_candidates.keys()))

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Optimizing stages {stage_names} using {self.algorithm} algorithm')
        await self.logger.spinner.append_message(f'Optimizer - {self.name} optimizing stages {stage_names} using {self.algorithm} algorithm')

        optimize_stages = [(
            stage.name, 
            stage
        ) for stage in optimization_candidates.values()]


        stages_count = len(optimization_candidates)

        # We may have less workers available during the optimize stage than assigned
        # to the execute stage, so store the original workers count for later.
        stage_workers_map = {
            stage.name: stage.workers for stage in optimization_candidates.values()
        }

        batched_stages: BatchedOptimzationCandidates = list(self.executor.partion_stage_batches(optimize_stages))

        return {
            'optimization_candidates': optimization_candidates,
            'stage_names': stage_names,
            'stages_count': stages_count,
            'stage_workers_map': stage_workers_map,
            'batched_stages': batched_stages
        }

    @event('collect_optimization_stages')
    async def create_optimization_configs(
        self,
        stages_count: int=0,
        batched_stages: BatchedOptimzationCandidates=[],
    ):
        batched_configs = []

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Batching optimization for - {stages_count} stages')

        serializable_context = self.context.as_serializable()

        for stage_name, stage, assigned_workers_count in batched_stages:

            configs = []
            stage_config = stage.client._config

            batch_size = int(stage_config.batch_size/assigned_workers_count)

            for worker_idx in range(assigned_workers_count):
                
                execute_stage_actions: List[ActionHook] = [hook.name for hook in stage.hooks[HookType.ACTION]]
                execute_stage_tasks: List[TaskHook] = [hook.name for hook in stage.hooks[HookType.TASK]]

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
                    'source_stage_target_events': self.linked_events,
                    'execute_stage_name': stage_name,
                    'execute_stage_generation_count': assigned_workers_count,
                    'execute_stage_id': stage.execution_stage_id,
                    'execute_stage_config': stage_config,
                    'execute_stage_batch_size': batch_size,
                    'execute_stage_plugins': execute_stage_plugins,
                    'execute_stage_linked_events': stage.linked_events,
                    'optimizer_iterations': self.optimize_iterations,
                    'optimizer_algorithm': self.algorithm,
                    'execute_stage_hooks': [
                        *execute_stage_actions,
                        *execute_stage_tasks
                    ],
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
            'batched_configs': batched_configs
        }

    @event('create_optimization_configs')
    async def execute_optimization(
        self,
        stages_count: int=0,
        batched_configs: Dict[str, Any]={},
    ):
        optimization_results = []
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting optimizaiton for - {stages_count} - stages')

        results = await self.executor.execute_batches(
            batched_configs,
            optimize_stage
        )

        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Completed optimizaiton for - {stages_count} - stages')

        for _, result in results:
            optimization_results.extend(result)

        return {
            'optimization_results': optimization_results
        }

    @event('execute_optimization')
    async def collect_optimized_batch_sizes(
        self,
        optimization_results: List[Any]=[],
    ):
        optimized_batch_sizes = []
        for optimization_result in optimization_results:
            optimized_config = optimization_result.get('config')
            optimized_batch_sizes.append(
                optimized_config.batch_size
            )

        optimized_batch_size = sum(optimized_batch_sizes)

        return {
            'optimized_batch_size': optimized_batch_size
        }

    @event('collect_optimized_batch_sizes')
    async def set_optimized_batch_size(
        self,
        optimization_results: List[Any]=[],
        optimization_candidates: Dict[str, Execute]={},
        optimized_batch_size: int=0
    ):
       
        stage_optimzations = {}
        stage_context = defaultdict(list)
        for optimization_result in optimization_results:
            
            stage_name = optimization_result.get('stage')
            optimized_config = optimization_result.get('config')

            stage = optimization_candidates.get(stage_name)
            stage.client._config = optimized_config

            action_hooks: List[ActionHook] = stage.hooks[HookType.ACTION]
            for hook in action_hooks:
                hook.session.pool.size = optimized_batch_size
                hook.session.sem = asyncio.Semaphore(optimized_batch_size)
                hook.session.pool.connections = []
                hook.session.pool.create_pool()
            
            pipeline_context = optimization_result.get('context', {})
            for context_key, context_value in pipeline_context.items():
                stage_context[context_key].append(context_value)

            stage_optimzations[stage_name] = optimized_batch_size

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Stage - {stage_name} - configured to use optimized batch size of - {optimized_batch_size} - VUs')

            optimized_config.optimized = True
            stage.optimized = True

        return {
            'stage_optimzations': stage_optimzations,
            'stage_context': stage_context
        }

    @context('set_optimized_batch_size')
    async def complete_optimization(
        self,
        stage_names: str=None,
        optimization_results: List[Any]=[],
        stage_workers_map: Dict[str, Execute]={},
        optimization_candidates: Dict[str, Execute]={},
        stage_optimzations: Dict[str, Union[int, float]]={},
        stage_context: Dict[str, Any]={}
    ):
        self.context[self.name] = stage_context

        for stage in optimization_candidates.values():
            stage.workers = stage_workers_map.get(stage.name)

        self.optimization_execution_time = round(time.monotonic() - self.optimization_execution_time_start)

        optimized_batch_sizes = ', '.join([
            f'{stage_name}: {optimized_batch_size}' for stage_name, optimized_batch_size in stage_optimzations.items()
        ])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Optimization complete for stages - {stage_names} - over - {self.optimization_execution_time} - seconds')
        await self.logger.spinner.set_default_message(f'Optimized - batch sizes for stages - {optimized_batch_sizes} - over {self.optimization_execution_time} seconds')
        
        return {
            'optimized_params': [
                result.get('params') for result in optimization_results
            ]
        }

