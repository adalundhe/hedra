import asyncio
import traceback
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)


async def execute_transition(current_stage: Stage, next_stage: Stage):

    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE)

    total_concurrent_execute_stages = []
    for generation_stage_name in current_stage.generation_stage_names:
        stage = execute_stages.get(generation_stage_name)
        
        if stage is not None:
            total_concurrent_execute_stages.append(stage)

    current_stage.total_concurrent_execute_stages = len(total_concurrent_execute_stages)
    
    if current_stage.timeout:
        execution_results = await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

    else:
        execution_results = await current_stage.run()

    if current_stage.context.results.get(current_stage.name) is None:
        current_stage.context.results[current_stage.name] = {
            'results': execution_results.get('results'),
            'total_elapsed': execution_results.get('total_elapsed')
        }
    
    else:
        current_stage.context.results[current_stage.name].update({
            'results': execution_results.get('results'),
            'total_elapsed': execution_results.get('total_elapsed')
        })

    current_stage.context.results_stages.append(current_stage)
    current_stage.context.visited.append(current_stage.name)
    
    next_stage.context = current_stage.context

    return current_stage, next_stage


async def execute_to_setup_transition(current_stage: Stage, next_stage: Stage):

    try:
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)
            execute_stages = list(current_stage.context.stages.get(StageTypes.EXECUTE).values())
            visited = current_stage.context.visited

            for stage in execute_stages:
                if stage.name not in visited and stage.state == StageStates.SETUP:
                    stage.state = StageStates.INITIALIZED

            current_stage.state = StageStates.EXECUTED

    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.SETUP


async def execute_to_execute_transition(current_stage: Stage, next_stage: Stage):

    try:
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)

            current_stage.state = StageStates.EXECUTED

    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.EXECUTE


async def execute_to_optimize_transition(current_stage: Stage, next_stage: Stage):
    
    try:
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)

            current_stage.state = StageStates.EXECUTED
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.OPTIMIZE


async def execute_to_teardown_transition(current_stage: Stage, next_stage: Stage):

    try:

        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)

            current_stage.state = StageStates.EXECUTED
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.TEARDOWN


async def execute_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    try:
        
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)

            current_stage.state = StageStates.EXECUTED
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.ANALYZE


async def execute_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)
            next_stage.data = current_stage.context.results

            next_stage.previous_stage = current_stage.name

            current_stage.state = StageStates.EXECUTED
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.CHECKPOINT


async def execute_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage)
            next_stage.data = current_stage.context.results[current_stage.name]

            next_stage.previous_stage = current_stage.name

            current_stage.state = StageStates.EXECUTED
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.WAIT