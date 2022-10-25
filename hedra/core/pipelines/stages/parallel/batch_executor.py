import asyncio
from concurrent.futures import ProcessPoolExecutor
import math
import dill
from types import FunctionType
import psutil
from typing import Any, Coroutine, Dict, List, Tuple, Union
from hedra.core.engines.types.common.base_result import BaseResult
from .synchronization import BatchedSemaphore



class BatchExecutor:

    def __init__(
        self, 
        max_workers: int = psutil.cpu_count(logical=False)
    ) -> None:
        self.max_workers = max_workers
        self.loop = asyncio.get_event_loop()
        self.sem = BatchedSemaphore(max_workers)
        self.pool = ProcessPoolExecutor(max_workers=max_workers)
        self.shutdown_task = None

    async def execute_batches(self, batched_stages: List[Tuple[int, List[Any]]], execution_task: FunctionType) -> List[Tuple[str, Any]]:

        return await asyncio.gather(*[
            asyncio.create_task(
                self._execute_stage(
                    stage_name,
                    assigned_workers_count,
                    execution_task,
                    configs
                )
            ) for stage_name, assigned_workers_count, configs in batched_stages
        ])


    async def _execute_stage(
        self, 
        stage_name: str,
        assigned_workers_count: int,
        execution_task: FunctionType,
        configs: List[List[Any]]
    ) -> Tuple[str, Any]:
        await self.sem.acquire(assigned_workers_count)
        stage_results = await self.execute_stage_batch(
            execution_task,
            configs
        )

        self.sem.release(assigned_workers_count)

        return (stage_name, stage_results)

    async def execute_stage_batch(
        self, 
        execution_task: FunctionType,
        configs: List[Any]
    ):
        results = await asyncio.gather(*[
            self.loop.run_in_executor(
                self.pool,
                execution_task,
                config
            ) for config in configs
        ])
        
        return results

    def partion_stage_batches(self, stages: List[Any], ) -> List[Tuple[str, Any, int]]:

        # How many batches do we have? For example -> 5 stages over 4
        # CPUs means 2 batches. The first batch will assign one stage to
        # each core. The second will assign all four cores to the remaing
        # one stage.    

        stages_count = len(stages)


        self.batch_sets_count = math.ceil(stages_count/self.max_workers)
        if self.batch_sets_count < 1:
            self.batch_sets_count = 1
        
        self.more_stages_than_cpus = stages_count/self.max_workers > 1

        if stages_count%self.max_workers > 0 and stages_count > self.max_workers:

            batch_size = self.max_workers
            workers_per_stage = int(self.max_workers/batch_size)
            batched_stages = [
                [
                    workers_per_stage for _ in range(batch_size)
                ] for _ in range(self.batch_sets_count - 1)
            ]

        else:
            batch_size = int(stages_count/self.batch_sets_count)
            workers_per_stage = int(self.max_workers/batch_size)
            batched_stages = [
                [
                    workers_per_stage for _ in range(batch_size)
                ] for _ in range(self.batch_sets_count)
            ]

        batches_count = len(batched_stages)

        # If we have a remainder batch - i.e. more stages than cores.
        last_batch = batched_stages[batches_count-1]

        last_batch_size = stages_count%self.max_workers
        if last_batch_size > 0 and self.more_stages_than_cpus:
            last_batch_workers = int(self.max_workers/last_batch_size)
            batched_stages.append([
                last_batch_workers for _ in range(last_batch_size)
            ])

        last_batch = batched_stages[self.batch_sets_count-1]
        last_batch_size = len(last_batch)
        last_batch_remainder = self.max_workers%last_batch_size


        if last_batch_remainder > 0:
            for idx in range(last_batch_remainder):
                last_batch[idx] += 1

        stage_idx = 0
        batches = []

        for batch in batched_stages:
            for stage_idx, stage_workers_count in enumerate(batch):

                stage_name, stage = stages[stage_idx]

                batches.append((
                    stage_name,
                    stage,
                    stage_workers_count
                ))

        return batches

