from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import IdleTranstionError

async def invalid_idle_transition(current_stage: Stage, next_stage: Stage):
    return IdleTranstionError(next_stage), StageTypes.ERROR

async def idle_to_setup_transition(current_stage: Stage, next_stage: Stage):
    await next_stage.run()
    
    return None, StageTypes.SETUP
