from hedra.core.graphs.hooks.types.internal import Internal
from .types.stage_types import StageTypes
from .stage import Stage


class Wait(Stage):
    stage_type=StageTypes.WAIT

    def __init__(self) -> None:
        super().__init__()

    @Internal
    async def run(self):
        pass