import asyncio
import functools
import statistics
from time import sleep
import psutil
import dill
from multiprocessing import get_context
from hedra.core.engines.client import Client
from concurrent.futures import ProcessPoolExecutor
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.personas import get_persona
from .parallel.types import PartitionMethod
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

        self.total_concurrent_execute_stages = 0
        self.execution_stage_id = 0
        self.optimized = False
        self.execute_setup_stage = None
        self.requires_shutdown = True

    @Internal
    async def run(self):

        loop = asyncio.get_running_loop()

        executor = ProcessPoolExecutor(
            max_workers=self.workers
        )

        if self.workers > 1 and self.total_concurrent_execute_stages == 1:                
    
            results_sets = await asyncio.gather(*[
                loop.run_in_executor(
                    executor,
                    execute_actions,
                    dill.dumps({
                        'partition_method': PartitionMethod.BATCHES,
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

        elif self.workers > 1 and self.total_concurrent_execute_stages > 1:

            if self.optimized is False:

                batch_size = self.client._config.batch_size
                stages_count = self.total_concurrent_execute_stages

                if stages_count > 1 and self.execution_stage_id == stages_count:
                    batch_size = int(batch_size/stages_count) + batch_size%stages_count

                else:
                    batch_size = int(batch_size/stages_count)

                self.client._config.batch_size = batch_size

            results_set = await loop.run_in_executor(
                executor,
                execute_actions,
                dill.dumps({
                    'partition_method': PartitionMethod.JOB_PER_CORE,
                    'workers': self.workers,
                    'worker_id': self.execution_stage_id,
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
            )

            results = results_set.get('results')
            total_elapsed = results_set.get('total_elapsed')

        else:

            persona = get_persona(self.client._config)
            persona.setup(self.hooks)

            results = await persona.execute()
            total_elapsed = persona.total_elapsed
            
        self._shutdown_task = loop.run_in_executor(None, executor.shutdown)

        return {
            'results': results,
            'total_elapsed': total_elapsed
        }