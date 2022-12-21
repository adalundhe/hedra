from typing import Any
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.transitions.exceptions import InvalidTransitionError
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.logging import HedraLogger


async def idle_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context

    return None, StageTypes.IDLE


async def invalid_transition(current_stage: Stage, next_stage: Stage):
    return InvalidTransitionError(current_stage, next_stage), StageTypes.ERROR


async def exit_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

    await current_stage.run()
    next_stage.context = None
    current_stage.context = None

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')

    return None, None

async def error_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Executing transition from {current_stage.name} to {next_stage.name}')

    await next_stage.run()
    next_stage.context = None
    current_stage.context = None

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - Completed transition from {current_stage.name} to {next_stage.name}')

    return None, None