import asyncio
import functools
import statistics
import psutil
import dill
from multiprocessing import get_context
from hedra.core.engines.client import Client
from concurrent.futures import ProcessPoolExecutor
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.personas import get_persona
from .parallel.execute_actions import execute_actions
from .stage import Stage



class Execute(Stage):
    stage_type=StageTypes.EXECUTE

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client = Client()
        
        self.accepted_hook_types = [ 
            HookType.SETUP, 
            HookType.BEFORE, 
            HookType.ACTION,
            HookType.AFTER,
            HookType.TEARDOWN,
            HookType.CHECK
        ]

        self.concurrent_execution_stages = []
        self.execution_stage_id = 0
        self.optimized = False

    @Internal
    async def run(self):
        
        if self.optimized is False:

            batch_size = self.client._config.batch_size
            stages_count = len(self.concurrent_execution_stages)

            if stages_count > 1 and self.execution_stage_id == stages_count:
                batch_size = int(batch_size/stages_count) + batch_size%stages_count

            else:
                batch_size = int(batch_size/stages_count)

            self.client._config.batch_size = batch_size

        print(self.name, self.client._config.batch_size)

        if self.workers > 1:

            loop = asyncio.get_running_loop()

            executor = ProcessPoolExecutor(
                max_workers=self.workers
            )
                
            
            results_sets = await asyncio.gather(*[
                loop.run_in_executor(
                    executor,
                    execute_actions,
                    dill.dumps({
                        'workers': self.workers,
                        'worker_id': idx + 1,
                        'config': self.client._config,
                        'hooks': [
                            {
                                'timeouts': hook.session.timeouts,
                                'reset_connections': hook.session.pool.reset_connections,
                                'hook_name': hook.name,
                                'stage': hook.stage,
                                'weight': hook.config.weight,
                                'order': hook.config.order,
                                **hook.action.to_serializable()
                            } for hook in self.hooks.get(HookType.ACTION)
                        ]
                    })
                ) for idx in range(self.workers)
            ])
            
            results = []
            elapsed_times = []
            for result_set in results_sets:
                results.extend(result_set.get('results'))
                elapsed_times.append(result_set.get('total_elapsed'))

            total_elapsed = statistics.median(elapsed_times)

        else:
            persona = get_persona(self.client._config)
            persona.setup(self.hooks)

            results = await persona.execute()
            total_elapsed = persona.total_elapsed
            

        return {
            'results': results,
            'total_elapsed': total_elapsed
        }