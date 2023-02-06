import uuid
from typing import Coroutine
from .analyze.analyze_edge import AnalyzeEdge, BaseEdge
from .checkpoint.checkpoint_edge import CheckpointEdge
from .common.complete_edge import CompleteEdge
from .common.error_edge import ErrorEdge
from .execute.execute_edge import ExecuteEdge
from .idle.idle_edge import IdleEdge
from .optimize.optimize_edge import OptimizeEdge
from .setup.setup_edge import SetupEdge
from .submit.submit_edge import SubmitEdge
from .teardown.teardown_edge import TeardownEdge
from .validate.validate_edge import ValidateEdge
from .wait.wait_edge import WaitEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes


class Transition:

    def __init__(self, transition: Coroutine, from_stage: Stage, to_stage: Stage) -> None:
        self.transition_id = str(uuid.uuid4())
        self.transition = transition
        self.from_stage = from_stage
        self.to_stage = to_stage

        edge_types = {
            StageTypes.ANALYZE: AnalyzeEdge,
            StageTypes.CHECKPOINT: CheckpointEdge,
            StageTypes.COMPLETE: CompleteEdge,
            StageTypes.ERROR: ErrorEdge,
            StageTypes.EXECUTE: ExecuteEdge,
            StageTypes.IDLE: IdleEdge,
            StageTypes.OPTIMIZE: OptimizeEdge,
            StageTypes.SETUP: SetupEdge,
            StageTypes.SUBMIT: SubmitEdge,
            StageTypes.TEARDOWN: TeardownEdge,
            StageTypes.VALIDATE: ValidateEdge,
            StageTypes.WAIT: WaitEdge
        }

        self.edge: BaseEdge = edge_types.get(from_stage.stage_type)(
            from_stage,
            to_stage
        )

    def assemble(self):
        return self.transition(self.from_stage, self.to_stage)

    async def execute(self):
        return await self.edge.transition()