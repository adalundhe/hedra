from typing import Any
from hedra.core.graphs.stages.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import IdleTranstionError
from hedra.core.graphs.transitions.exceptions import StageExecutionError


async def invalid_idle_transition(current_stage: Stage, next_stage: Stage):
    return IdleTranstionError(next_stage), StageTypes.ERROR


async def idle_to_validate_transition(current_stage: Stage, next_stage: Stage):

    try:
        next_stage.context = current_stage.context

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.VALIDATE

async def idle_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:
        next_stage.context = current_stage.context

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.WAIT