from typing import List, Dict, Any, Tuple
from collections import defaultdict
from hedra.core.graphs.stages.base.stage import Stage
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


            if len(destinations)> 1:
                transition.edge.split([transition.edge for transition in destinations])

                transition.destinations = [
                    destination_transition.edge.destination.name for destination_transition in destinations
                ]

            

            

