import asyncio
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)


async def setup_transition(current_stage: Stage, next_stage: Stage):
    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE).items()
    paths = current_stage.context.paths.get(current_stage.name)
    visited = current_stage.context.visited

    execute_stages = {
        stage_name: stage for stage_name, stage in execute_stages if stage_name in paths and stage_name not in visited
    }

    setup_stages = {}
    for execute_stage_name, execute_stage in execute_stages.items():
        if execute_stage.state == StageStates.INITIALIZED:
            execute_stage.state = StageStates.SETTING_UP
            setup_stages[execute_stage_name] = execute_stage

  
    current_stage.stages = setup_stages

    if current_stage.timeout:

        setup_stages = await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

    else:

        setup_stages = await current_stage.run()

    for execute_stage in setup_stages.values():
        execute_stage.state = StageStates.SETUP

    current_stage.context.stages[StageTypes.EXECUTE].update(setup_stages)
    current_stage.context.reporting_config = current_stage.reporting_config

    next_stage.context = current_stage.context


async def setup_to_validate_transition(current_stage: Stage, next_stage: Stage):
    
    try:

        await setup_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.VALIDATE


async def setup_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    try:

        await setup_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.OPTIMIZE


async def setup_to_execute_transition(current_stage: Stage, next_stage: Stage):

    try:

        await setup_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.EXECUTE


async def setup_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        await setup_transition(current_stage, next_stage)

        next_stage.data = {
            'stages': current_stage.context.stages[StageTypes.EXECUTE],
            'reporter_config': current_stage.context.reporting_config
        }
        
        next_stage.previous_stage = current_stage.name
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR
    
    return None, StageTypes.CHECKPOINT
