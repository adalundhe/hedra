from __future__  import annotations
import asyncio
from typing import List
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.error.error import Error
from hedra.core.graphs.stages.types.stage_states import StageStates


class ErrorEdge(BaseEdge[Error]):

    def __init__(self, source: Error, destination: BaseEdge[Stage]) -> None:
        super(
            ErrorEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):

        await self.destination.run()

        self.source.state = StageStates.ERRORED

        self.visited.append(self.source.name)

        return None, None
    
    def _update(self, destination: Stage):
        self.next_history.update({
            (self.source.name, destination.name): {}
        })
    
    def split(self, edges: List[ErrorEdge]) -> None:
        pass
