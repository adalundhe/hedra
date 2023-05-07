import asyncio
import math
import multiprocessing
import psutil
import warnings
from multiprocessing import active_children
from types import FunctionType
from concurrent.futures.process import BrokenProcessPool
from concurrent.futures import ProcessPoolExecutor
from hedra.core.graphs.stages.base.exceptions.process_killed_error import ProcessKilledError
from typing import (
    Any, 
    List, 
    Tuple, 
    Dict
)
from .synchronization import BatchedSemaphore
from .stage_priority import StagePriority


class BatchExecutor:

    def __init__(
        self, 
        max_workers: int = psutil.cpu_count(logical=False),
        start_method: str='spawn'
    ) -> None:
        self.max_workers = max_workers
        self.loop = asyncio.get_event_loop()
        self.start_method = start_method

        self.context = multiprocessing.get_context(start_method)
        self.sem = BatchedSemaphore(max_workers)
        self.pool = ProcessPoolExecutor(max_workers=psutil.cpu_count(logical=False), mp_context=self.context)
        self.shutdown_task = None
        self.batch_by_stages = False

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

        try:
            return await asyncio.gather(*[
                self.loop.run_in_executor(
                    self.pool,
                    execution_task,
                    config
                    
                ) for config in configs
            ])
        
        except BrokenProcessPool:
            raise ProcessKilledError()


    def partion_stage_batches(self, stages: List[Any], ) -> List[Tuple[str, Any, int]]:

        # How many batches do we have? For example -> 5 stages over 4
        # CPUs means 2 batches. The first batch will assign one stage to
        # each core. The second will assign all four cores to the remaing
        # one stage.    

        batches = []

        if self.batch_by_stages is False:

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

            for batch in batched_stages:
                for stage_idx, stage_workers_count in enumerate(batch):

                    stage_name, stage = stages[stage_idx]

                    batches.append((
                        stage_name,
                        stage,
                        stage_workers_count
                    ))

        else:

            for stage_name, stage in stages:
                batches.append((
                    stage_name,
                    stage,
                    1
                ))

        return batches
    
    def partion_prioritized_stage_batches(self, transitions: List[Any], ) -> List[Tuple[str, str, str, int]]:

        # How many batches do we have? For example -> 5 stages over 4
        # CPUs means 2 batches. The first batch will assign one stage to
        # each core. The second will assign all four cores to the remaing
        # one stage.    

        batches = []
        seen_transitions: List[Any] = []


        sorted_transitions = list(sorted(
            transitions,
            key=lambda transition: transition.edge.source.priority_level.value
        ))


        bypass_partition_batch: List[Any] = []
        for transition in sorted_transitions:
            if transition.edge.skip_stage or transition.edge.source.allow_parallel is False:
                bypass_partition_batch.append((
                    transition.edge.source.name,
                    transition.edge.destination.name,
                    transition.edge.source.priority,
                    0
                ))

                seen_transitions.append(transition)

        batches.append(bypass_partition_batch)

        
        stages = {
            transition.edge.source.name: transition.edge.source for transition in sorted_transitions
        }

        parallel_stages_count = len([
            stage for stage in stages.values() if stage.allow_parallel
        ])

        stages_count = len(stages)

        auto_stages_count = len([
            stage for stage in stages.values() if stage.priority_level == StagePriority.AUTO
        ])

        if parallel_stages_count == 1:
            
            parallel_transitions = [
                transition for transition in sorted_transitions if transition.edge.skip_stage is False
            ]

            transition = parallel_transitions.pop()

            transition_group = [(
                transition.edge.source.name,
                transition.edge.destination.name,
                transition.edge.source.priority,
                transition.edge.source.workers
            )]

            return [transition_group]

        elif auto_stages_count == stages_count and parallel_stages_count > 0:
            transition_group = [
                (
                    transition.edge.source.name,
                    transition.edge.destination.name,
                    transition.edge.source.priority,
                    transition.edge.source.workers
                ) for transition in sorted_transitions if transition.edge.skip_stage is False
            ]

            return [transition_group]
            
        else:

            min_workers_counts: Dict[str, int] = {}
            max_workers_counts: Dict[str, int] = {}

            for transition in sorted_transitions:

                if transition.edge.skip_stage is False and transition.edge.source.allow_parallel:
                    worker_allocation_range: Tuple[int, int] = StagePriority.get_worker_allocation_range(
                        transition.edge.source.priority_level,
                        self.max_workers
                    )

                    minimum_workers, maximum_workers = worker_allocation_range
                    min_workers_counts[transition.edge.source.name] = minimum_workers
                    max_workers_counts[transition.edge.source.name] = maximum_workers

            for transition in sorted_transitions:
                
                if transition not in seen_transitions:


                    # So for example 8 - 4 = 4 we need another stage with 4
                    batch_workers_allocated = max_workers_counts.get(transition.edge.source.name)
                    transition_group = [(
                        transition.edge.source.name,
                        transition.edge.destination.name,
                        transition.edge.source.priority,
                        batch_workers_allocated
                    )]

                    for other_transition in sorted_transitions:
                        
                        if other_transition != transition and transition not in seen_transitions:
                            
                            transition_workers_allocated = max_workers_counts.get(other_transition.edge.source.name)
                            min_workers = min_workers_counts.get(other_transition.edge.source.name)

                            current_allocation = batch_workers_allocated + transition_workers_allocated

                            while current_allocation > self.max_workers and transition_workers_allocated >= min_workers:
                                transition_workers_allocated -= 1
                                current_allocation = batch_workers_allocated + transition_workers_allocated

                            if current_allocation <= self.max_workers and transition_workers_allocated > 0:

                                batch_workers_allocated += transition_workers_allocated
                                transition_group.append((
                                    other_transition.edge.source.name,
                                    other_transition.edge.destination.name,
                                    other_transition.edge.source.priority,
                                    transition_workers_allocated
                                ))

                                seen_transitions.append(other_transition)

                    batches.append(transition_group)
                    seen_transitions.append(transition)

        return batches

    async def shutdown(self):

        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self.pool.shutdown()

            child_processes = active_children()
            for child in child_processes:
                child.kill()

    def close(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self.pool.shutdown()

            child_processes = active_children()
            for child in child_processes:
                child.kill()