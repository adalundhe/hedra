from typing import List
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.idle.idle import Idle
from hedra.core.graphs.simple_context import SimpleContext


class IdleEdge(BaseEdge[Idle]):

    def __init__(self, source: Idle, destination: Stage) -> None:
        super(
            IdleEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):

        await self.source.run()

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self._update(self.destination)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        self.next_history.update({
            (self.source.name, destination.name): {}
        })

    def split(self, edges: List[BaseEdge]) -> None:
        pass