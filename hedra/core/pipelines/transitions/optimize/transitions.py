import asyncio
import traceback
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

        execute_stages = execute_stage.context.stages.get(StageTypes.EXECUTE)

        for generation_stage_name in execute_stage.generation_stage_names:
            stage = execute_stages.get(generation_stage_name)
            
            if stage is not None:
                execute_stage.concurrent_execution_stages.append(stage)

        for execution_stage_idx, exeuction_stage in enumerate(execute_stage.concurrent_execution_stages):
            exeuction_stage.execution_stage_id = execution_stage_idx + 1
        
        current_stage.concurrent_execution_stages = list(execute_stage.concurrent_execution_stages)
        current_stage.execution_stage_id = execute_stage.execution_stage_id
        current_stage.execute_stage_config = execute_stage.client._config
        current_stage.execute_stage_hooks = execute_stage.hooks

        if current_stage.timeout:
            optimized_config, optimized_hooks = await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

        else:
            optimized_config, optimized_hooks = await current_stage.run()

        execute_stage.client._config = optimized_config
        execute_stage.hooks = optimized_hooks
        execute_stage.optimized = True

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


async def optimize_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        await optimize_transition(current_stage, next_stage)

        next_stage.data = next_stage.context.optimized_params
        next_stage.previous_stage = current_stage.name
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.WAIT
