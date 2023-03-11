import asyncio
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.graphs.stages.base.stage import Stage


class Complete(Stage):
    stage_type=StageTypes.COMPLETE

    def __init__(self) -> None:
        super().__init__()

    @Internal()
    async def run(self):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Graph has reached terminal point')
