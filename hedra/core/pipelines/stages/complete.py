from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Complete(Stage):
    stage_type=StageTypes.COMPLETE

    def __init__(self) -> None:
        super().__init__()