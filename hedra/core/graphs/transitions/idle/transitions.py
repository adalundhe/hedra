from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.exceptions import IdleTranstionError
from hedra.core.graphs.transitions.exceptions import StageExecutionError
from hedra.logging import HedraLogger


async def invalid_idle_transition(current_stage: Stage, next_stage: Stage):
    return IdleTranstionError(next_stage), StageTypes.ERROR


async def idle_to_validate_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:
        
        await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
        await current_stage.run()

        if next_stage.context is None:
            next_stage.context = SimpleContext()

        for known_key in current_stage.context.known_keys:
            next_stage.context[known_key] = current_stage.context[known_key]

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.VALIDATE

async def idle_to_wait_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    try:

        await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
        await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
        await current_stage.run()

        if next_stage.context is None:
            next_stage.context = SimpleContext()
            
        for known_key in current_stage.context.known_keys:
            next_stage.context[known_key] = current_stage.context[known_key]

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR

    else:
        return None, StageTypes.WAIT