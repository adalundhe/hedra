class ErrorEdge:

    def __init__(self) -> None:
        self.wants = []
        self.provides = []

        self.values = {}
        self.paths = []

    async def connect(self):
        pass


import asyncio
from hedra.core.graphs.simple_context import SimpleContext
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
        
        if self.timeout:
            await asyncio.wait_for(self.destination.source.run(), timeout=self.timeout)
        
        else:
            await self.destination.run()

        self.source.state = StageStates.ERRORED

        self.visited.append(self.source.name)

        return None, None
