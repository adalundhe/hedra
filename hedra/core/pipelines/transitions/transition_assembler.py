import asyncio
from typing import Dict, List
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types import StageTypes
from .transition import Transition
from .common import (
    idle_transition,
    invalid_transition,
    exit_transition
)
from .idle import (
    invalid_idle_transition,
    idle_to_setup_transition,
)

from .setup import (
    setup_to_optimize_transition,
    setup_to_execute_transition,
    setup_to_checkpoint_transition,
)

from .optimize import (
    optimize_to_execute_transition,
    optimize_to_checkpoint_transition,
)

from .execute import (
    execute_to_execute_transition,
    execute_to_optimize_transition,
    execute_to_teardown_transition,
    execute_to_analyze_transition,
    execute_to_checkpoint_transition,
)

from .teardown import (
    teardown_to_analyze_transition,
    teardown_to_checkpoint_transition
)

from .analyze import (
    analyze_to_checkpoint_transition
)

from .checkpoint import (
    checkpoint_to_setup_transition,
    checkpoint_to_optimize_transition,
    checkpoint_to_execute_transition,
    checkpoint_to_teardown_transition,
    checkpoint_to_analyze_transition,
    checkpoint_to_complete_transition
)


class TransitionAssembler:
    transition_types = {

        # State: Idle
        (StageTypes.IDLE, StageTypes.IDLE): idle_transition,
        (StageTypes.IDLE, StageTypes.SETUP): idle_to_setup_transition,
        (StageTypes.IDLE, StageTypes.OPTIMIZE): invalid_idle_transition,
        (StageTypes.IDLE, StageTypes.EXECUTE):  invalid_idle_transition,
        (StageTypes.IDLE, StageTypes.TEARDOWN): invalid_idle_transition,
        (StageTypes.IDLE, StageTypes.ANALYZE): invalid_idle_transition,
        (StageTypes.IDLE, StageTypes.CHECKPOINT): invalid_idle_transition,
        (StageTypes.IDLE, StageTypes.COMPLETE): invalid_idle_transition,
        (StageTypes.IDLE, StageTypes.ERROR): invalid_idle_transition,

        # State: Setup
        (StageTypes.SETUP, StageTypes.SETUP): invalid_transition,
        (StageTypes.SETUP, StageTypes.IDLE): invalid_transition,
        (StageTypes.SETUP, StageTypes.OPTIMIZE): setup_to_optimize_transition,
        (StageTypes.SETUP, StageTypes.EXECUTE): setup_to_execute_transition,
        (StageTypes.SETUP, StageTypes.TEARDOWN): invalid_transition,
        (StageTypes.SETUP, StageTypes.ANALYZE): invalid_transition,
        (StageTypes.SETUP, StageTypes.CHECKPOINT): setup_to_checkpoint_transition,
        (StageTypes.SETUP, StageTypes.COMPLETE): invalid_transition,
        (StageTypes.SETUP, StageTypes.ERROR): invalid_transition,

        # State: Optimize
        (StageTypes.OPTIMIZE, StageTypes.OPTIMIZE): invalid_transition,
        (StageTypes.OPTIMIZE, StageTypes.IDLE): invalid_transition,
        (StageTypes.OPTIMIZE, StageTypes.SETUP): invalid_transition,
        (StageTypes.OPTIMIZE, StageTypes.EXECUTE): optimize_to_execute_transition,
        (StageTypes.OPTIMIZE, StageTypes.TEARDOWN): invalid_transition,
        (StageTypes.OPTIMIZE, StageTypes.ANALYZE): invalid_transition,
        (StageTypes.OPTIMIZE, StageTypes.CHECKPOINT): optimize_to_checkpoint_transition,
        (StageTypes.OPTIMIZE, StageTypes.COMPLETE): invalid_transition,
        (StageTypes.OPTIMIZE, StageTypes.ERROR): invalid_transition,

        # State: Execute
        (StageTypes.EXECUTE, StageTypes.EXECUTE): execute_to_execute_transition,
        (StageTypes.EXECUTE, StageTypes.IDLE): invalid_transition,
        (StageTypes.EXECUTE, StageTypes.SETUP): invalid_transition,
        (StageTypes.EXECUTE, StageTypes.OPTIMIZE): execute_to_optimize_transition,
        (StageTypes.EXECUTE, StageTypes.TEARDOWN): execute_to_teardown_transition,
        (StageTypes.EXECUTE, StageTypes.ANALYZE): execute_to_analyze_transition,
        (StageTypes.EXECUTE, StageTypes.CHECKPOINT): execute_to_checkpoint_transition,
        (StageTypes.EXECUTE, StageTypes.COMPLETE): invalid_transition,
        (StageTypes.EXECUTE, StageTypes.ERROR): invalid_transition,

        # State: Teardown
        (StageTypes.TEARDOWN, StageTypes.TEARDOWN): invalid_transition,
        (StageTypes.TEARDOWN, StageTypes.IDLE): invalid_transition,
        (StageTypes.TEARDOWN, StageTypes.SETUP): invalid_transition,
        (StageTypes.TEARDOWN, StageTypes.OPTIMIZE): invalid_transition,
        (StageTypes.TEARDOWN, StageTypes.EXECUTE): invalid_transition,
        (StageTypes.TEARDOWN, StageTypes.ANALYZE): teardown_to_analyze_transition,
        (StageTypes.TEARDOWN, StageTypes.CHECKPOINT): teardown_to_checkpoint_transition,
        (StageTypes.TEARDOWN, StageTypes.COMPLETE): invalid_transition,
        (StageTypes.TEARDOWN, StageTypes.ERROR): invalid_transition,

        # State: Analyze
        (StageTypes.ANALYZE, StageTypes.ANALYZE): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.IDLE): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.SETUP): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.OPTIMIZE): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.EXECUTE): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.TEARDOWN): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.CHECKPOINT): analyze_to_checkpoint_transition,
        (StageTypes.ANALYZE, StageTypes.COMPLETE): invalid_transition,
        (StageTypes.ANALYZE, StageTypes.ERROR): invalid_transition,

        # State: Checkpoint
        (StageTypes.CHECKPOINT, StageTypes.CHECKPOINT): invalid_transition,
        (StageTypes.CHECKPOINT, StageTypes.IDLE): invalid_transition,
        (StageTypes.CHECKPOINT, StageTypes.SETUP): checkpoint_to_setup_transition,
        (StageTypes.CHECKPOINT, StageTypes.OPTIMIZE): checkpoint_to_optimize_transition,
        (StageTypes.CHECKPOINT, StageTypes.EXECUTE): checkpoint_to_execute_transition,
        (StageTypes.CHECKPOINT, StageTypes.TEARDOWN): checkpoint_to_teardown_transition,
        (StageTypes.CHECKPOINT, StageTypes.ANALYZE): checkpoint_to_analyze_transition,
        (StageTypes.CHECKPOINT, StageTypes.COMPLETE): checkpoint_to_complete_transition,
        (StageTypes.CHECKPOINT, StageTypes.ERROR): invalid_transition,

        # State: Complete
        (StageTypes.COMPLETE, StageTypes.COMPLETE): exit_transition,
        (StageTypes.COMPLETE, StageTypes.IDLE): exit_transition,
        (StageTypes.COMPLETE, StageTypes.SETUP): exit_transition,
        (StageTypes.COMPLETE, StageTypes.OPTIMIZE): exit_transition,
        (StageTypes.COMPLETE, StageTypes.EXECUTE): exit_transition,
        (StageTypes.COMPLETE, StageTypes.TEARDOWN): exit_transition,
        (StageTypes.COMPLETE, StageTypes.ANALYZE): exit_transition,
        (StageTypes.COMPLETE, StageTypes.CHECKPOINT): exit_transition,
        (StageTypes.COMPLETE, StageTypes.ERROR): exit_transition,

        # State: Error
        (StageTypes.ERROR, StageTypes.ERROR): exit_transition,
        (StageTypes.ERROR, StageTypes.IDLE): exit_transition,
        (StageTypes.ERROR, StageTypes.SETUP): exit_transition,
        (StageTypes.ERROR, StageTypes.OPTIMIZE): exit_transition,
        (StageTypes.ERROR, StageTypes.EXECUTE): exit_transition,
        (StageTypes.ERROR, StageTypes.TEARDOWN): exit_transition,
        (StageTypes.ERROR, StageTypes.ANALYZE): exit_transition,
        (StageTypes.ERROR, StageTypes.CHECKPOINT): exit_transition,
        (StageTypes.ERROR, StageTypes.COMPLETE): exit_transition
    }

    def __init__(self) -> None:
        self.transitions = {}
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def build_transitions_graph(self, topological_generations: List[List[str]], stages: Dict[str, Stage]):

        transitions_groups: List[Dict[str, List[Transition]]] = []

        generated_stages = {stage_name: stage() for stage_name, stage in stages.items()}

        reversed_topological_generations = topological_generations[::-1]
        for generation in reversed_topological_generations[:-1]:
         
            transition_group = {}
            for stage_name in generation:

                stage_instance = generated_stages.get(stage_name)
                dependencies = stage_instance.dependencies

                for dependency in dependencies:
                    
                    dependency_name = dependency.__name__
                    dependency_instance = generated_stages.get(dependency_name)

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