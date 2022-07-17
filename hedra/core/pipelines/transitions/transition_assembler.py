import asyncio
import networkx
from types import SimpleNamespace
from typing import Dict, List
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .transition import Transition
from .common import (
    invalid_transition
)
from .idle import (
    invalid_idle_transition
)


class TransitionAssembler:

    def __init__(self, transition_types) -> None:
        self.transition_types = transition_types
        self.generated_stages = {}
        self.transitions = {}
        self.instances_by_type = {}
        self.loop = asyncio.get_event_loop()

    def generate_stages(self, stages: Dict[str, Stage]):
        self.instances_by_type = {}

        for stage in stages.values():
            self.instances_by_type[stage.stage_type] = []

        self.generated_stages = {stage_name: stage() for stage_name, stage in stages.items()}

        for stage in self.generated_stages.values():
            self.instances_by_type[stage.stage_type].append(stage)

    def build_transitions_graph(self, topological_generations: List[List[str]]):

        transitions_groups: List[Dict[str, List[Transition]]] = []
        

        reversed_topological_generations = topological_generations[::-1]
        for generation in reversed_topological_generations[:-1]:
         
            transition_group = {}
            for stage_name in generation:

                stage_instance = self.generated_stages.get(stage_name)
                dependencies = stage_instance.dependencies

                for dependency in dependencies:
                    
                    dependency_name = dependency.__name__
                    dependency_instance = self.generated_stages.get(dependency_name)

                    transition = self.transition_types.get((
                        dependency_instance.stage_type,
                        stage_instance.stage_type
                    ))

                    if transition == invalid_transition or transition == invalid_idle_transition:
                        invalid_transition_error, _ = self.loop.run_until_complete(transition(dependency, stage_instance))
                        raise invalid_transition_error

                                        
                    if transition_group.get(dependency_name) is None:
                        transition_group[dependency_name] = [
                            Transition(
                                transition,
                                dependency_instance,
                                stage_instance
                            )
                        ]

                    else:
                        transition_group[dependency_name].append(
                            Transition(
                                transition,
                                dependency_instance,
                                stage_instance
                            )
                        )

            transitions_groups.append(transition_group)


        transitions_groups = transitions_groups[::-1]
        
        return transitions_groups

    def map_to_setup_stages(self, graph: networkx.DiGraph):

        for setup_stage in self.instances_by_type.get(StageTypes.SETUP):
            setup_stage.context = SimpleNamespace()
            setup_stage.context.stages = {}
            setup_stage.context.setup = []
            setup_stage_name = setup_stage.__class__.__name__

            for stage_type in StageTypes:

                setup_stage.context.stages[stage_type] = {}

                for stage in self.instances_by_type.get(stage_type, []):

                    stage_name = stage.__class__.__name__

                    has_path = networkx.has_path(
                        graph, 
                        setup_stage_name,
                        stage_name
                    )

                    if has_path:
                        setup_stage.context.stages[stage_type][stage_name] = stage
