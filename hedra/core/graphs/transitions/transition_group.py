from typing import List, Dict
from collections import defaultdict
from hedra.core.graphs.stages.base.stage import Stage
from .transition import Transition


class TransitionGroup:

    def __init__(self) -> None:
        self.transitions: List[Transition]  = []
        self.transitions_by_type: Dict[str, List[Transition]] = defaultdict(list)
        self.destination_groups: Dict[str, Dict[str, List[Transition]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self.targets: Dict[str, Stage] = {}

    @property
    def count(self):
        return len(self.transitions)

    def __iter__(self):
        for transition in self.transitions:
            yield transition

    def add_transition(self, transition: Transition):
        self.destination_groups[transition.to_stage.name][transition.from_stage.stage_type].append(transition)
        self.transitions_by_type[transition.from_stage.stage_type].append(transition)
        self.transitions.append(transition)
    
    def sort_and_map_transitions(self):


        for transition in self.transitions:
            transitions_of_type = self.destination_groups[transition.to_stage.name][transition.from_stage.stage_type][1:]
            transition_count = len(transitions_of_type)

            # If we have multiple transitions to the same stage
            # from stages of the same type and the target does not support
            # multiple transitions, "fold" the transitions based on 
            # how the edge supports folding.
            if transition_count > 1 and transition.metadata.allow_multiple_edges is False and transition.edge.folded is False:
                first_transition = transitions_of_type[0]

                for additional_transition in transitions_of_type[1:]:
                    first_transition.edge.fold(additional_transition.edge)

                    additional_transition.edge.folded = True


