import asyncio
from typing import Dict, Any
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.wait.wait import Wait
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


class WaitEdge(BaseEdge[Wait]):

    def __init__(self, source: Wait, destination: Stage) -> None:
        super(
            WaitEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):
        pass