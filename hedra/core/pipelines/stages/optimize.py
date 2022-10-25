import asyncio
import dill
from typing import Dict, List, Tuple
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.engines.client.time_parser import TimeParser
from .parallel.optimize_stage import optimize_stage
from .parallel.batch_executor import BatchExecutor
from .execute import Execute
from .stage import Stage


class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    optimize_iterations=0
    optimizer_type='shg'
    stage_time_limit='1m'
    
    def __init__(self) -> None:
        super().__init__()
        self.generation_optimization_candidates = 0
        self.execution_stage_id = 0

        self.results = None

        time_parser = TimeParser(self.stage_time_limit)
        self.time_limit = time_parser.time
        self.requires_shutdown = True
        self.allow_parallel = True

    @Internal
    async def run(self, stages: Dict[str, Execute]):

        optimization_results = []

        # We may have less workers available during the optimize stage than assigned
        # to the execute stage, so store the original workers count for later.
        stage_workers_map = {
            stage.name: stage.workers for stage in stages.values()
        }

        optimize_stages = [(
            stage.name, 
            stage
        ) for stage in stages.values()]

        batched_stages: List[Tuple[str, Execute, int]] = list(self.executor.partion_stage_batches(optimize_stages))
        batched_configs = []

        for stage_name, stage, assigned_workers_count in batched_stages:

            configs = []
            stage_config = stage.client._config

            batch_size = int(stage_config.batch_size/assigned_workers_count)

            for worker_idx in range(assigned_workers_count):
                configs.append({
                    'worker_idx': worker_idx,
                    'execute_stage_name': stage_name,
                    'execute_stage_generation_count': assigned_workers_count,
                    'execute_stage_id': stage.execution_stage_id,
                    'execute_stage_config': stage_config,
                    'execute_stage_batch_size': batch_size,
                    'optimizer_iterations': self.optimize_iterations,
                    'optimizer_type': self.optimizer_type,
                    'execute_stage_hooks': [
                        hook.name for hook in stage.hooks.get(HookType.ACTION)
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


        results = await self.executor.execute_batches(
            batched_configs,
            optimize_stage
        )

        for _, result in results:
            optimization_results.extend(result)

        optimized_batch_sizes = []
        for optimization_result in optimization_results:
            optimized_config = optimization_result.get('config')
            optimized_batch_sizes.append(
                optimized_config.batch_size
            )

        optimized_batch_size = sum(optimized_batch_sizes)

        for optimization_result in optimization_results:
            
            stage_name = optimization_result.get('stage')
            optimized_config = optimization_result.get('config')

            stage = stages.get(stage_name)

            stage.client._config = optimized_config

            for hook in stage.hooks.get(HookType.ACTION):
                hook.session.pool.size = optimized_batch_size
                hook.session.sem = asyncio.Semaphore(optimized_config.batch_size)
                hook.session.pool.connections = []
                hook.session.pool.create_pool()

            stage.optimized = True

        for stage in stages.values():
            stage.workers = stage_workers_map.get(stage.name)

        return [
            result.get('params') for result in optimization_results
        ]

