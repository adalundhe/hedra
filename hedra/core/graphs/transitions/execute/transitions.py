import asyncio
import traceback
from typing import Dict
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.execute.execute import Execute
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)
from hedra.logging import HedraLogger


async def execute_transition(current_stage: Execute, next_stage: Stage, logger: HedraLogger):
    
    current_stage.state = StageStates.EXECUTING

    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE)
    analyze_stages: Dict[str, Stage] = current_stage.context.stages.get(StageTypes.ANALYZE)
    paths = current_stage.context.paths

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

    total_concurrent_execute_stages = []
    for generation_stage_name in current_stage.generation_stage_names:
        stage = execute_stages.get(generation_stage_name)
        
        if stage is not None:
            total_concurrent_execute_stages.append(stage)

    current_stage.total_concurrent_execute_stages = len(total_concurrent_execute_stages)
    if current_stage.timeout:
        await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

    else:
        await current_stage.run()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    
    if next_stage.context is None:
        next_stage.context = SimpleContext()

    for known_key in current_stage.context.known_keys:
        next_stage.context[known_key] = current_stage.context[known_key]

    if next_stage.context['results'] is None:
        next_stage.context['results'] = {}

    if next_stage.context['results_stages'] is None:
        next_stage.context['results_stages'] = []

    if next_stage.context['visited'] is None:
        next_stage.context['visited']  = []


    next_stage.context['results'].update({
        current_stage.name: ResultsSet({
            'results': current_stage.context['stage_results'],
            'total_results': current_stage.context['total_results'],
            'total_elapsed': current_stage.context['total_elapsed']
        })
    })

    for stage in analyze_stages.values():
        if stage.name in paths.get(current_stage.name) and stage.state == StageStates.INITIALIZED:
            if stage.context is None:
                stage.context = SimpleContext()

            if stage.context['results'] is None:
                stage.context['results'] = {}

            if stage.context['results_stages'] is None:
                stage.context['results_stages'] = []

            if stage.context['visited'] is None:
                stage.context['visited']  = []


            stage.context['results'].update({
                current_stage.name: ResultsSet({
                    'results': current_stage.context['stage_results'],
                    'total_results': current_stage.context['total_results'],
                    'total_elapsed': current_stage.context['total_elapsed']
                })
            })

    next_stage.context['results_stages'].append(current_stage)
    next_stage.context['visited'].append(current_stage.name)

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