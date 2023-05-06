import asyncio
import signal
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.batch_executor import BatchExecutor
from hedra.core.graphs.stages.base.parallel.synchronization import BatchedSemaphore
from hedra.core.graphs.stages.types.stage_types import StageTypes
from .transition import Transition
from .common.base_edge import BaseEdge


HistoryUpdate = Dict[Tuple[str, str], Any]


class TransitionGroup:

    def __init__(self) -> None:
        self.transitions: List[Transition]  = []
        self.transitions_by_type: Dict[StageTypes, List[Transition]] = defaultdict(list)
        self.destination_groups: Dict[Tuple[str, StageTypes], List[Transition]] = defaultdict(list)
        self.targets: Dict[str, Stage] = {}

        self.edges_by_name: Dict[str, BaseEdge] = {}
        self.adjacency_list: Dict[str, List[Transition]] = []
        self.transition_idx = 0
        self.cpu_pool_size = 0
        self._sem: BatchedSemaphore = None
        self._batched_transitions: List[List[Transition]] = []
        self._transition_configs: Dict[Tuple[str, str], int] = {}
        self._executors = []

    @property
    def count(self):
        return len(self.transitions)

    def __iter__(self):
        for transition in self.transitions:
            yield transition

    def add_transition(self, transition: Transition):
        transition.edge.transition_idx = self.transition_idx
        self.destination_groups[(transition.from_stage.name, transition.to_stage.stage_type)].append(transition)
        self.transitions_by_type[transition.from_stage.stage_type].append(transition)
        self.transitions.append(transition)

        self.transition_idx += 1
    
    def sort_and_map_transitions(self):

        for transition in self.transitions:

            transition.edges = [
                group_transition.edge for group_transition in self.transitions
            ]

            destinations = self.adjacency_list[transition.edge.source.name]
            transition.destinations = [
                transition.edge.destination.name
            ]


            transition.edge.source.priority

            if len(destinations)> 1:
                transition.edge.setup()
                transition.edge.split([transition.edge for transition in destinations])

                transition.destinations = [
                    destination_transition.edge.destination.name for destination_transition in destinations
                ]

        executor = BatchExecutor(max_workers=self.cpu_pool_size)
        self._batched_transitions: List[List[Transition]] = executor.partion_prioritized_stage_batches(
            self.transitions
        )

        for group in self._batched_transitions:
            for source, destination, _, workers in group:
                self._transition_configs[(source, destination)] = workers

    async def execute_group(self):
        try:
            self._sem = BatchedSemaphore(self.cpu_pool_size)
            return await asyncio.gather(*[
                asyncio.create_task(
                    self._acquire_transition_lock(transition)
                ) for transition in self.transitions
            ])
        
        except KeyboardInterrupt:
            return []
    
    async def _acquire_transition_lock(self, transition: Transition) -> Any:

        workers = self._transition_configs.get((
            transition.edge.source.name,
            transition.edge.destination.name
        ))
    
        if workers:

            loop = asyncio.get_event_loop()

            transition.edge.source.workers = workers
            transition.edge.source.executor.max_workers = workers

            def handle_loop_stop(signame):
                try:
                    self._sem.release(workers)
                except BrokenPipeError:
                    pass

                except RuntimeError:
                    pass

                except Exception:
                    pass

            for signame in ('SIGINT', 'SIGTERM'):
                loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(signame)
                )

            await self._sem.acquire(workers)
            result = await transition.execute()
            self._sem.release(workers)

        else:
            result = await transition.execute()

        return result
