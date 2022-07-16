from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage

class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT