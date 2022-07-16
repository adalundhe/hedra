from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.transitions.exceptions import InvalidTransitionError
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def idle_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.results
    return None, StageTypes.IDLE

async def invalid_transition(current_stage: Stage, next_stage: Stage):
    return InvalidTransitionError(current_stage, next_stage), StageTypes.ERROR

async def exit_transition(current_stage: Stage, next_stage: Stage):
    return None, None
