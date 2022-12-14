from hedra.core.graphs.hooks.types.internal import Internal
from .types.stage_types import StageTypes
from .stage import Stage


class Wait(Stage):
    stage_type=StageTypes.WAIT

    def __init__(self) -> None:
        super().__init__()

    @Internal()
    async def run(self):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Waiting for Stage group to complete')
        await self.logger.spinner.append_message(f'Stage - {self.name} - Waiting for Stage group to complete')