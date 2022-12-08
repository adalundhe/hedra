import asyncio
from typing import Any
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.stages.stage import Stage
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)

async def teardown_transition(current_stage: Stage, next_stage: Stage):
    teardown_hooks = []
    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE).values()
    paths = current_stage.context.paths

    valid_states = [
        StageStates.EXECUTED
    ]
    
    if current_stage.state == StageStates.INITIALIZED:

        current_stage.state = StageStates.TEARDOWN_INITIALIZED

        for stage in execute_stages:
            in_path = current_stage.name in paths.get(stage.name)

            if stage.state in valid_states and in_path:
                stage.state = StageStates.TEARDOWN_INITIALIZED

                stage_teardown_hooks = stage.hooks.get(HookType.TEARDOWN)
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

    next_stage.context = current_stage.context


async def teardown_to_analyze_transition(current_stage: Stage, next_stage: Stage):
    
    try:

        await teardown_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.ANALYZE


async def teardown_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        next_stage.previous_stage = current_stage.name
        next_stage.context = current_stage.context

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