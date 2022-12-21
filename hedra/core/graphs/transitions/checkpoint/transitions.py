import asyncio
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)
from hedra.logging import HedraLogger


async def checkpoint_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    if current_stage.state == StageStates.INITIALIZED:

        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

        current_stage.state = StageStates.CHECKPOINTING
        
        if current_stage.timeout:
            await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)
        
        else:
            await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED

        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')

    else:
        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context


async def checkpoint_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage)
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.CHECKPOINT


async def checkpoint_to_setup_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage)
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.SETUP


async def checkpoint_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage)
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.OPTIMIZE


async def checkpoint_to_execute_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage)
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.EXECUTE


async def checkpoint_to_teardown_transition(current_stage: Stage, next_stage: Stage):
    
    try:

        await checkpoint_transition(current_stage, next_stage)
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.TEARDOWN


async def checkpoint_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage)
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.ANALYZE


async def checkpoint_to_submit_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage), StageTypes.ERROR
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.SUBMIT
    

async def checkpoint_to_complete_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage), StageTypes.ERROR
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.COMPLETE


async def checkpoint_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        await checkpoint_transition(current_stage, next_stage), StageTypes.ERROR
        
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.WAIT