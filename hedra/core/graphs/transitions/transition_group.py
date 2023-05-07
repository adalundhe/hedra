import asyncio
from typing import (
    List, 
    Dict, 
    Any, 
    Tuple
)
from collections import defaultdict
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.base.parallel.batch_executor import BatchExecutor
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
        self.transitions_by_edge: Dict[Tuple[str, str,], Transition] = {}

        self.edges_by_name: Dict[str, BaseEdge] = {}
        self.adjacency_list: Dict[str, List[Transition]] = []
        self.transition_idx = 0
        self.cpu_pool_size = 0
        self._batched_transitions: List[List[Transition]] = []
        self._transition_configs: Dict[Tuple[str, str, str, int], int] = {}
        self._executors: List[BatchExecutor] = []

    @property
    def count(self):
        return len(self.transitions_by_edge)

    def __iter__(self):
        for transition in self.transitions_by_edge.values():
            yield transition

    def add_transition(self, transition: Transition):
        transition.edge.transition_idx = self.transition_idx
        self.destination_groups[(transition.from_stage.name, transition.to_stage.stage_type)].append(transition)
        self.transitions_by_type[transition.from_stage.stage_type].append(transition)
        self.transitions_by_edge[(transition.edge.source.name, transition.edge.destination.name)] = transition

        self.transition_idx += 1
    
    def sort_and_map_transitions(self):

        for edge_key, transition in self.transitions_by_edge.items():

            transition.edges = [
                group_transition.edge for group_transition in self.transitions_by_edge.values()
            ]

            destinations = self.adjacency_list[transition.edge.source.name]
            transition.destinations = [
                transition.edge.destination.name
            ]

            if len(destinations)> 1:
                transition.edge.setup()
                transition.edge.split([transition.edge for transition in destinations])

                transition.destinations = [
                    destination_transition.edge.destination.name for destination_transition in destinations
                ]

            self.transitions_by_edge[edge_key] = transition

        executor = BatchExecutor(max_workers=self.cpu_pool_size)
        batched_transitions = executor.partion_prioritized_stage_batches(
            list(self.transitions_by_edge.values())
        )

        prioritized_groups: List[Tuple[int, Transition]] = []
        for group in batched_transitions:
            
            group_priorities: List[int] = []
            for transition_config in group:
                source, destination, _, _ = transition_config
                
                transition = self.transitions_by_edge.get((
                    source,
                    destination
                ))

                transition_priority: StagePriority = transition.edge.source.priority_level

                if transition_priority == StagePriority.AUTO:
                    group_priorities.append(0)

                else:
                    group_priorities.append(transition_priority.value)

            group_priority = 0
            if len(group_priorities) > 0:
                group_priority = max(group_priorities)
                

            prioritized_groups.append((
                group_priority,
                group
            ))

        sorted_batches = list(sorted(
            prioritized_groups,
            key=lambda prioritized_group: prioritized_group[0],
            reverse=True
        ))

        self._batched_transitions = [
            group for _, group in sorted_batches
        ]

        for group in self._batched_transitions:
            for source, destination, _, workers in group:
                self._transition_configs[(source, destination)] = workers

        executor.close()

    async def execute_group(self):
        results: List[Any] = []

        for group in self._batched_transitions:
            group_results = await asyncio.gather(*[
                asyncio.create_task(
                    self._execute_transition(transition_config)
                ) for transition_config in group
            ])

            results.extend(group_results)

        return results
    
    async def _execute_transition(self, transition_config: Tuple[str, str, str, int]) -> Any:

        source, destination, _, workers = transition_config

        transition = self.transitions_by_edge.get((
            source,
            destination
        ))
    
        if workers > 0:
            transition.edge.source.workers = workers
            executor = BatchExecutor(max_workers=workers)
            transition.edge.source.executor = executor
            self._executors.append(executor)

        return await transition.execute()
