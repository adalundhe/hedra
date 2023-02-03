import asyncio
import traceback
from collections import defaultdict
from typing import Dict
from hedra.core.graphs.simple_context import SimpleContext
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

    valid_states = [
        StageStates.INITIALIZED,
        StageStates.VALIDATED
    ]

    if current_stage.state in valid_states:

        current_stage.state = StageStates.EXECUTING
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

        setup_candidates: Dict[str, Execute] = {}
        
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

        current_stage.context['setup_stages'] = setup_candidates
        current_stage.context['setup_config'] = current_stage.config

        if current_stage.timeout:

            await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

        else:

            await current_stage.run()

        if next_stage.context is None:
            next_stage.context = SimpleContext()

        for known_key in current_stage.context.known_keys:
            next_stage.context[known_key] = current_stage.context[known_key]

        for execute_stage in setup_candidates.values():
            execute_stage.state = StageStates.SETUP

            if execute_stage.context is None:
                execute_stage.context = SimpleContext()

            for known_key in current_stage.context.known_keys:
                execute_stage.context[known_key] = current_stage.context[known_key]
            
            execute_stage.context.create_if_not_exists('stages', defaultdict(dict))
            execute_stage.context['stages'][StageTypes.EXECUTE].update(current_stage.context['ready_stages'])

            setup_execute_stage: Execute = current_stage.context['ready_stages'].get(execute_stage.name)
            if setup_execute_stage:
                execute_stage.context['execute_hooks'] = setup_execute_stage.context['execute_hooks']
            execute_stage.context.create_or_update('setup_stages', list(setup_candidates.keys()), [])

            execute_stage.context['setup_config'] = current_stage.context['setup_config']
            execute_stage.context['setup_by'] = current_stage.name

        await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
        
        current_stage.state = StageStates.COMPLETE


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