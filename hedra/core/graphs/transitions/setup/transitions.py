import asyncio
from typing import Dict, List
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup import Setup
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)
from hedra.logging import HedraLogger


async def setup_transition(current_stage: Setup, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE).items()
    paths = current_stage.context.paths.get(current_stage.name)
    path_lengths: Dict[str, int] = current_stage.context.path_lengths.get(current_stage.name)
    visited = current_stage.context.visited

    execute_stages: Dict[str, Execute] = {
        stage_name: stage for stage_name, stage in execute_stages if stage_name in paths and stage_name not in visited
    }

    setup_candidates = {}
    
    setup_stages: Dict[str, Setup] = current_stage.context.stages.get(StageTypes.SETUP)
    execute_stages: Dict[str, Execute] = current_stage.context.stages.get(StageTypes.EXECUTE)
    following_setup_stage_distances = [
        path_length for stage_name, path_length in path_lengths.items() if stage_name in setup_stages
    ]

    for stage_name in path_lengths.keys():
        stage_distance = path_lengths.get(stage_name)

        if stage_name in execute_stages:

            if len(following_setup_stage_distances) > 0 and stage_distance < min(following_setup_stage_distances):
                setup_candidates[stage_name] = execute_stages.get(stage_name)

            elif len(following_setup_stage_distances) == 0:
                setup_candidates[stage_name] = execute_stages.get(stage_name)

    current_stage.generation_setup_candidates = len(setup_candidates)
    stages = {}
    next_stage_decendants = current_stage.context.paths.get(next_stage.name)
    path_decendants = [
        next_stage.name,
        *next_stage_decendants
    ]

    if len(current_stage.apply_to_stages) > 0:
        for stage_name in current_stage.apply_to_stages:
            execute_stage: Execute = execute_stages.get(stage_name)
            if execute_stage.state == StageStates.INITIALIZED and execute_stage.name in path_decendants:
                execute_stage.state = StageStates.SETTING_UP
                stages[stage_name] = execute_stage

    else:

        generation_setup_candidates = [
            stage_name for stage_name in current_stage.generation_stage_names if stage_name in current_stage.context.stages.get(StageTypes.SETUP)
        ]

        user_specified_setup_candidates = []
        all_setup_stages: Dict[str, Setup] = current_stage.context.stages.get(StageTypes.SETUP)
        for stage_name in generation_setup_candidates:
            stage: Setup = all_setup_stages.get(stage_name)

            user_specified_setup_candidates.extend(stage.apply_to_stages)

        for execute_stage_name, execute_stage in setup_candidates.items():
            if execute_stage.state == StageStates.INITIALIZED and execute_stage.name in path_decendants and execute_stage.name not in user_specified_setup_candidates:
                execute_stage.state = StageStates.SETTING_UP
                stages[execute_stage_name] = execute_stage

    current_stage.stages = stages

    if current_stage.timeout:

        setup_results = await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

    else:

        setup_results = await current_stage.run()

    for execute_stage in stages.values():
        execute_stage.state = StageStates.SETUP

    current_stage.context.stages[StageTypes.EXECUTE].update(setup_results)

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    
    next_stage.context = current_stage.context
    current_stage = None


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


async def setup_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        await setup_transition(current_stage, next_stage)
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    return None, StageTypes.WAIT