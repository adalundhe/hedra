from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.stages.base.stage import Stage
from hedra.logging import HedraLogger


async def wait_to_wait_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.WAIT


async def wait_to_setup_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.SETUP


async def wait_to_validate_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.VALIDATE


async def wait_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.OPTIMIZE


async def wait_to_execute_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.EXECUTE


async def wait_to_teardown_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.TEARDOWN


async def wait_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.ANALYZE


async def wait_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.CHECKPOINT


async def wait_to_submit_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.SUBMIT


async def wait_to_complete_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context
    return None, StageTypes.COMPLETE


async def wait_to_error_transition(current_stage: Stage, next_stage: Stage):

    logger = HedraLogger()
    logger.initialize()

    await logger.spinner.system.debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')
    await logger.filesystem.aio['hedra.core'].debug(f'{current_stage.metadata_string} - NoOp transition from {current_stage.name} to {next_stage.name}')

    next_stage.context = current_stage.context 
    next_stage.context = current_stage.context
    return None, StageTypes.ERROR