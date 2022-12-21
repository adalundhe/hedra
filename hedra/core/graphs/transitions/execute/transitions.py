import asyncio
import traceback
from typing import Dict, List, Union
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.logging import HedraLogger


async def execute_transition(current_stage: Stage, next_stage: Stage, logger: HedraLogger):

    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE)

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

    total_concurrent_execute_stages = []
    for generation_stage_name in current_stage.generation_stage_names:
        stage = execute_stages.get(generation_stage_name)
        
        if stage is not None:
            total_concurrent_execute_stages.append(stage)

    current_stage.total_concurrent_execute_stages = len(total_concurrent_execute_stages)
    
    if current_stage.timeout:
        execution_results: Dict[str, Union[List[BaseResult], int, float]] = await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

    else:
        execution_results: Dict[str, Union[List[BaseResult], int, float]] = await current_stage.run()

    if current_stage.context.results.get(current_stage.name) is None:
        current_stage.context.results[current_stage.name] = {
            'results': execution_results.get('results'),
            'total_results': execution_results.get('total_results'),
            'total_elapsed': execution_results.get('total_elapsed')
        }
    
    else:
        current_stage.context.results[current_stage.name].update({
            'results': execution_results.get('results'),
            'total_results': execution_results.get('total_results'),
            'total_elapsed': execution_results.get('total_elapsed')
        })

    current_stage.context.results_stages.append(current_stage)
    current_stage.context.visited.append(current_stage.name)

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    
    next_stage.context = current_stage.context

    return current_stage, next_stage


async def execute_to_setup_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)
            execute_stages = list(current_stage.context.stages.get(StageTypes.EXECUTE).values())
            visited = current_stage.context.visited

            for stage in execute_stages:
                if stage.name not in visited and stage.state == StageStates.SETUP:
                    stage.state = StageStates.INITIALIZED

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')

    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.SETUP


async def execute_to_execute_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')

    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.EXECUTE


async def execute_to_optimize_transition(current_stage: Stage, next_stage: Stage):
    
    logger = HedraLogger()
    logger.initialize()

    try:
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR
    
    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.OPTIMIZE


async def execute_to_teardown_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:

        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.TEARDOWN


async def execute_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:
        
        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.ANALYZE


async def execute_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:

        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)
            next_stage.data = current_stage.context.results

            next_stage.previous_stage = current_stage.name

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.CHECKPOINT


async def execute_to_wait_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:

        valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        if current_stage.state in valid_states:
            current_stage.state = StageStates.EXECUTING

            current_stage, next_stage = await execute_transition(current_stage, next_stage, logger)
            next_stage.data = current_stage.context.results[current_stage.name]

            next_stage.previous_stage = current_stage.name

            current_stage.state = StageStates.EXECUTED

        else:
            await logger.spinner.system.debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Skipping transition from {current_stage.name} to {next_stage.name}')
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    current_stage = None

    return None, StageTypes.WAIT