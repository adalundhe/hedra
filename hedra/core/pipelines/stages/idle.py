from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.hooks.types.internal import Internal
from .stage import Stage

class Idle(Stage):
    stage_type=StageTypes.IDLE

    def __init__(self) -> None:
        super().__init__()
