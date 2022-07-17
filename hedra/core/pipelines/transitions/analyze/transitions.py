from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def analyze_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    next_stage.context = current_stage.context
    return None, StageTypes.CHECKPOINT
