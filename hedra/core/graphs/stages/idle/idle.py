import asyncio
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.stages.base.stage import Stage

class Idle(Stage):
    stage_type=StageTypes.IDLE

    def __init__(self) -> None:
        super().__init__()
        self.name = self.__class__.__name__

    @Internal()
    async def run(self):
        await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Starting graph execution')
        await asyncio.sleep(1)
        
