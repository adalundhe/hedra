import asyncio
from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)


async def optimize_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.INITIALIZED,
        StageStates.SETUP
    ]

    if next_stage.state in valid_states:
        next_stage.state = StageStates.OPTIMIZING

        next_stage_name = next_stage.name
        execute_stage = current_stage.context.stages.get(
            StageTypes.EXECUTE
        ).get(next_stage_name)
        
        current_stage.persona = execute_stage.persona

        if current_stage.timeout:
            optimized_persona = await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

        else:
            optimized_persona = await current_stage.run()

        next_stage.persona = optimized_persona
        next_stage.context.optimized_params = current_stage.results

        next_stage.state = StageStates.OPTIMIZED

        next_stage.context = current_stage.context


async def optimize_to_execute_transition(current_stage: Stage, next_stage: Stage):

    try:

        await optimize_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.EXECUTE


async def optimize_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        await optimize_transition(current_stage, next_stage)

        next_stage.data = next_stage.context.optimized_params
        next_stage.previous_stage = current_stage.name
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.CHECKPOINT
