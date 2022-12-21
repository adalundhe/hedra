import asyncio
from typing import Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.checkpoint import Checkpoint
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)
from hedra.logging import HedraLogger


async def teardown_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    teardown_hooks = []
    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE).values()
    paths = current_stage.context.paths

    valid_states = [
        StageStates.EXECUTED
    ]
    
    if current_stage.state == StageStates.INITIALIZED:

        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

        current_stage.state = StageStates.TEARDOWN_INITIALIZED

        for stage in execute_stages:
            in_path = current_stage.name in paths.get(stage.name)

            if stage.state in valid_states and in_path:
                stage.state = StageStates.TEARDOWN_INITIALIZED

                stage_teardown_hooks = stage.hooks[HookType.TEARDOWN]
                if stage_teardown_hooks:
                    teardown_hooks.extend(stage_teardown_hooks)

        current_stage.hooks[HookType.ACTION] = teardown_hooks

        if current_stage.timeout:
            await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

        else:
            await current_stage.run()

        for stage in execute_stages:
            in_path = current_stage.name in paths.get(stage.name)

            if stage.state == StageStates.TEARDOWN_INITIALIZED and in_path:
                stage.state = StageStates.TEARDOWN_COMPLETE

        current_stage.state = StageStates.TEARDOWN_COMPLETE
        
        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')

    else:
        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context


async def teardown_to_analyze_transition(current_stage: Stage, next_stage: Stage):
    
    try:

        await teardown_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.ANALYZE


async def teardown_to_checkpoint_transition(current_stage: Stage, next_stage: Checkpoint):

    try:

        await teardown_transition(current_stage, next_stage)

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR
        
    return None, StageTypes.CHECKPOINT


async def teardown_to_wait_transition(current_stage: Stage, next_stage: Stage):
    
    try:

        await teardown_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.WAIT