from __future__ import annotations
import uuid
from typing import List, Dict, Tuple, Any
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes
from .analyze.analyze_edge import AnalyzeEdge, BaseEdge
from .checkpoint.checkpoint_edge import CheckpointEdge
from .common.transtition_metadata import TransitionMetadata
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


HistoryUpdate = Dict[Tuple[str, str], Any]


class Transition:

    def __init__(self, metadata: TransitionMetadata, from_stage: Stage, to_stage: Stage) -> None:
        self.transition_id = str(uuid.uuid4())
        self.metadata = metadata
        self.from_stage = from_stage
        self.to_stage = to_stage
        self.edges_by_name: Dict[Tuple[str, str], BaseEdge] = {}
        self.adjacency_list: Dict[str, List[Transition]] = []
        self.predecessors = []
        self.descendants = []
        self.transition_idx = 0
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

    async def execute(self):
        
        result = await self.edge.transition()


        if self.to_stage.stage_type is not StageTypes.COMPLETE:
            source_name = self.edge.source.name
            destination_name = self.edge.destination.name

            neighbors: Tuple[str, str] = [
                (destination_name, transition.edge.destination.name) for transition in self.adjacency_list[destination_name]
            ]

            source_history: HistoryUpdate = self.edge.next_history[(source_name, destination_name)]

            for neighbor in neighbors:
                required_keys = self.edges_by_name[neighbor].requires
                self.edges_by_name[neighbor].from_stage_name = source_name

                self.edges_by_name[neighbor].history.update({
                    (source_name, destination_name): {
                        key: value for key, value in source_history.items() if key in required_keys
                    }
                })

        return result
