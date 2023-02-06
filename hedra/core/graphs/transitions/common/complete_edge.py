import asyncio
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.complete.complete import Complete
from hedra.core.graphs.stages.types.stage_states import StageStates


class CompleteEdge(BaseEdge[Complete]):

    def __init__(self, source: Complete, destination: BaseEdge[Stage]) -> None:
        super(
            CompleteEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):
        
        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        
        else:
            await self.source.run()

        self.source.state = StageStates.COMPLETE

        return None, None
