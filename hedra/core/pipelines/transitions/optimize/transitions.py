import asyncio
from sys import path
import traceback
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)


async def optimize_transition(current_stage: Stage, next_stage: Stage):

    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE).items()
    paths = current_stage.context.paths.get(current_stage.name)

    path_lengths = current_stage.context.path_lengths.get(current_stage.name)

    visited = current_stage.context.visited

    execute_stages = {
        stage_name: stage for stage_name, stage in execute_stages if stage_name in paths and stage_name not in visited
    }

    optimization_candidates = {}
    
    setup_stages = current_stage.context.stages.get(StageTypes.SETUP)
    optimize_stages = current_stage.context.stages.get(StageTypes.OPTIMIZE)

    for stage_name in path_lengths.keys():

        execute_stage = execute_stages.get(stage_name)

        if execute_stage:
            execute_stage_paths = current_stage.context.paths.get(stage_name)
            
            for decendant_stage_name in execute_stage_paths:
                if decendant_stage_name in setup_stages or decendant_stage_name in optimize_stages:
                    optimization_candidates[stage_name] = execute_stage

    current_stage.generation_optimization_candidates = len(optimization_candidates)

    valid_states = [
        StageStates.INITIALIZED,
        StageStates.SETUP,
    ]

    stages = {}

    next_stage_decendants = current_stage.context.paths.get(next_stage.name)
    path_decendants = [
        next_stage.name,
        *next_stage_decendants
    ]

    for execute_stage_name, execute_stage in optimization_candidates.items():


        if execute_stage.state in valid_states and execute_stage.name in path_decendants:
            execute_stage.state = StageStates.OPTIMIZING
            stages[execute_stage_name] = execute_stage

    if current_stage.timeout:
        optimization_results = await asyncio.wait_for(current_stage.run(stages), timeout=current_stage.timeout)

    else:
        optimization_results = await current_stage.run(stages)

    next_stage.context.optimized_params = optimization_results
    next_stage.state = StageStates.OPTIMIZED

    next_stage.context = current_stage.context


async def optimize_to_execute_transition(current_stage: Stage, next_stage: Stage):

    try:

        await optimize_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    
    current_stage = None

    return None, StageTypes.EXECUTE


async def optimize_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    try:

        next_stage.previous_stage = current_stage.name

        await optimize_transition(current_stage, next_stage)

        next_stage.data = next_stage.context.optimized_params
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.CHECKPOINT


async def optimize_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        next_stage.previous_stage = current_stage.name
        
        await optimize_transition(current_stage, next_stage)

        next_stage.data = next_stage.context.optimized_params
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.WAIT
