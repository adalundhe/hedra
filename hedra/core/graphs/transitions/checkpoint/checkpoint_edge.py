import asyncio
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.checkpoint.checkpoint import Checkpoint
from hedra.core.graphs.stages.types.stage_states import StageStates

class CheckpointEdge(BaseEdge[Checkpoint]):

    def __init__(self, source: Checkpoint, destination: BaseEdge[Stage]) -> None:
        super(
            CheckpointEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):
        self.source.state = StageStates.CHECKPOINTING
        
        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        
        else:
            await self.source.run()

        self.destination.update(self.history)

        self.source.state = StageStates.CHECKPOINTED

        if self.destination.source.context is None:
            self.destination.source.context = SimpleContext()

        return None, self.destination.source.stage_type