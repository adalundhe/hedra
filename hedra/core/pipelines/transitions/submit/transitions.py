import asyncio
import traceback
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)


async def submit_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.SUBMITTING
        
        current_stage.summaries = current_stage.context.summaries

        if current_stage.timeout:
            await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)
        
        else:
            await current_stage.run()

        current_stage.state = StageStates.SUBMITTED
        next_stage.state = StageStates.SUBMITTED
    
    next_stage.context = current_stage.context


async def submit_to_setup_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR
    
    return None, StageTypes.SETUP


async def submit_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.OPTIMIZE


async def submit_to_execute_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
        
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.EXECUTE


async def submit_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR
        
    return None, StageTypes.CHECKPOINT


async def submit_to_complete_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        print(traceback.format_exc())
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.COMPLETE


async def submit_to_submit_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.SUBMIT


async def submit_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        await submit_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.WAIT
