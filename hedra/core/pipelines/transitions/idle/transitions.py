from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_types import StageTypes

async def idle_to_setup_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.results
    return None, StageTypes.SETUP
