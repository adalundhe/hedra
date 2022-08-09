import asyncio
import networkx
import inspect
from typing import Dict, List
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.error import Error
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.simple_context import SimpleContext
from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.core.pipelines.hooks.types.hook import Hook
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

            methods = inspect.getmembers(stage, predicate=inspect.ismethod) 

            for _, method in methods:

                method_name = method.__qualname__
                hook: Hook = registrar.all.get(method_name)
                
                if hook:
                    hook.call = hook.call.__get__(stage, stage.__class__)
                    setattr(stage, hook.shortname, hook.call)

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
        
        idle_stages = self.instances_by_type.get(StageTypes.IDLE)
        for idle_stage in idle_stages:
            idle_stage.context = SimpleContext()
            idle_stage.context.stages = {}
            idle_stage.context.visited = []
            idle_stage.context.results = {}
            idle_stage.context.results_stages = []
            idle_stage.context.summaries = {}
            idle_stage.context.paths = {}
            
        idle_stage_name = idle_stage.__class__.__name__

        complete_stage = self.instances_by_type.get(StageTypes.COMPLETE)[0]

        for stage_type in StageTypes:

            idle_stage.context.stages[stage_type] = {}

            for stage in self.instances_by_type.get(stage_type, []):

                stage_name = stage.__class__.__name__

                has_path = networkx.has_path(
                    graph, 
                    idle_stage_name,
                    stage_name
                )

                if has_path:
                    idle_stage.context.stages[stage_type][stage_name] = stage
                    paths = networkx.all_shortest_paths(graph, stage_name, complete_stage.name)

                    stage_paths = []
                    for path in paths:
                        stage_paths.extend(path)
                    
                    idle_stage.context.paths[stage_name] = stage_paths

    def create_error_transition(self, error: Exception):

        from_stage = error.from_stage
            
        error_transition = self.transition_types.get((
            from_stage.stage_type,
            StageTypes.ERROR
        ))

        error_stage = Error()
        error_stage.error = error

        return Transition(
            error_transition,
            from_stage,
            error_stage
        )
                    