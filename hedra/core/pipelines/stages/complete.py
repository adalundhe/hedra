import asyncio
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.hooks.types.internal import Internal
from .stage import Stage


class Complete(Stage):
    stage_type=StageTypes.COMPLETE

    def __init__(self) -> None:
        super().__init__()

    @Internal
    async def run(self):
        pass