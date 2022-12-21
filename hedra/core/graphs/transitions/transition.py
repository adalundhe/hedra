import uuid
from typing import Coroutine
from hedra.core.graphs.stages.base.stage import Stage


class Transition:

    def __init__(self, transition: Coroutine, from_stage: Stage, to_stage: Stage) -> None:
        self.transition_id = str(uuid.uuid4())
        self.transition = transition
        self.from_stage = from_stage
        self.to_stage = to_stage

    def assemble(self):
        return self.transition(self.from_stage, self.to_stage)