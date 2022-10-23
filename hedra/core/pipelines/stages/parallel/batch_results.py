import asyncio
import math
import psutil
from typing import Any, Coroutine, Dict, List, Tuple, Union
from hedra.core.engines.types.common.base_result import BaseResult
from .synchronization import BatchedSemaphore



class BatchedResults:

    def __init__(
        self, 
        stages: List[Any], 
        max_workers: int = psutil.cpu_count(logical=False)
    ) -> None:
        self.max_workers = max_workers

        self.sem = BatchedSemaphore(max_workers)
        self.stages = stages
        
        self.stages_count = len(stages)
        self.batch_sets_count = math.ceil(self.stages_count/max_workers)
        
        self.more_stages_than_cpus = self.stages_count/max_workers > 1

    async def execute_batches(self, batched_stages: List[Tuple[str, Any, int]], execution_task: Coroutine, **kwargs: Any):

        return await asyncio.gather(*[
            asyncio.create_task(
                self._execute_stage(
                    stage_name,
                    stage_results,
                    assigned_workers_count,
                    execution_task,
                    **kwargs
                )
            ) for stage_name, stage_results, assigned_workers_count in batched_stages
        ])


    async def _execute_stage(
        self, 
        stage_name: str, 
        stage_results: Dict[str, Union[int, List[BaseResult]]],
        assigned_workers_count: int,
        execution_task: Coroutine,
        **kwargs
    ):
        await self.sem.acquire(assigned_workers_count)
        stage_result = await execution_task(
            stage_name,
            stage_results,
            assigned_workers_count=assigned_workers_count,
            **kwargs
        )

        self.sem.release(assigned_workers_count)

        return stage_result

    def partion_stage_batches(self) -> List[Tuple[str, Any, int]]:

        # How many batches do we have? For example -> 5 stages over 4
        # CPUs means 2 batches. The first batch will assign one stage to
        # each core. The second will assign all four cores to the remaing
        # one stage.    

        if self.stages_count%self.max_workers > 0 and self.stages_count > self.max_workers:

            batch_size = self.max_workers
            workers_per_stage = int(self.max_workers/batch_size)
            batched_stages = [
                [
                    workers_per_stage for _ in range(batch_size)
                ] for _ in range(self.batch_sets_count - 1)
            ]

        else:
            batch_size = int(self.stages_count/self.batch_sets_count)
            workers_per_stage = int(self.max_workers/batch_size)
            batched_stages = [
                [
                    workers_per_stage for _ in range(batch_size)
                ] for _ in range(self.batch_sets_count)
            ]

        batches_count = len(batched_stages)

        # If we have a remainder batch - i.e. more stages than cores.
        last_batch = batched_stages[batches_count-1]

        last_batch_size = self.stages_count%self.max_workers
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

                stage_name, stage = self.stages[stage_idx]

                batches.append((
                    stage_name,
                    stage,
                    stage_workers_count
                ))

        return batches

