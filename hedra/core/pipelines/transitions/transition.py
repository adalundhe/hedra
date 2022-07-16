from typing import Any, Coroutine, List
from hedra.core.pipelines.stages.stage import Stage


class Transition:

    def __init__(self, transition: Coroutine, from_stage: Stage, to_stage: Stage) -> None:

        self.transition = transition
        self.from_stage = from_stage
        self.to_stage = to_stage

    def assemble(self):
        return self.transition(self.from_stage, self.to_stage)