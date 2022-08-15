import asyncio
from typing import Dict
import dill
from concurrent.futures import ProcessPoolExecutor
from ..hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.pipelines.stages.optimizers import Optimizer
from hedra.core.personas import get_persona
from .parallel.optimize_stage import optimize_stage
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

    @Internal
    async def run(self, stages: Dict[str, Execute]):

        loop = asyncio.get_running_loop()
        optimization_results = []
        executor = ProcessPoolExecutor(self.workers)


        if self.generation_optimization_candidates > 1:

            results = await asyncio.gather(*[
                loop.run_in_executor(
                    executor,
                    optimize_stage,
                    dill.dumps({
                        'execute_stage_name': stage_name,
                        'execute_stage_generation_count': stage.total_concurrent_execute_stages,
                        'execute_stage_id': stage.execution_stage_id,
                        'execute_stage_config': stage.client._config,
                        'optimizer_iterations': self.optimize_iterations,
                        'optimizer_type': self.optimizer_type,
                        'execute_stage_hooks': [
                            hook.name for hook in stage.hooks.get(HookType.ACTION)
                        ],
                        'time_limit': self.time_limit
                    })
                ) for stage_name, stage in stages.items()
            ])

            for result in results:
                optimization_results.append(result)

        else:
            for stage_name, stage in stages.items():
                persona = get_persona(stage.client._config)
                persona.setup(stage.hooks)
   
                optimizer = Optimizer({
                    'iterations': self.optimize_iterations,
                    'algorithm': self.optimizer_type,
                    'persona': persona,
                    'time_limit': self.time_limit
                })

                results = await loop.run_in_executor(
                    None,
                    optimizer.optimize,
                    loop
                )

                optimization_results.append(result)

        for optimization_result in optimization_results:

            stage_name = optimization_result.get('stage')
            optimized_config = optimization_result.get('config')

            stage = stages.get(stage_name)

            stage.client._config = optimized_config

            for hook in stage.hooks.get(HookType.ACTION):
                hook.session.concurrency = optimized_config.batch_size
                hook.session.pool.size = optimized_config.batch_size
                hook.session.sem = asyncio.Semaphore(optimized_config.batch_size)
                hook.session.pool.connections = []
                hook.session.pool.create_pool()

            stage.optimized = True

        
        self._shutdown_task = loop.run_in_executor(None, executor.shutdown)
        return [
            result.get('params') for result in results
        ]

