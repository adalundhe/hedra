from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage

class Error(Stage):
    stage_type=StageTypes.ERROR

    def __init__(self) -> None:
        super().__init__()