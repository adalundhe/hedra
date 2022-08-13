import asyncio
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.transitions.exceptions import (
    StageExecutionError,
    StageTimeoutError
)
from hedra.core.pipelines.stages.exceptions import (
    HookValidationError,
    ReservedMethodError,
    MissingReservedMethodError
)


async def validate_to_setup_transition(current_stage: Stage, next_stage: Stage):

    try:

        if current_stage.state == StageStates.INITIALIZED:
            current_stage.state = StageStates.VALIDATING
            current_stage.stages = current_stage.context.stages
            
            if current_stage.timeout:
                await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

            else:
                await current_stage.run()
            
            current_stage.state = StageStates.VALIDATED

        next_stage.state = StageStates.VALIDATED
        next_stage.context = current_stage.context

    except MissingReservedMethodError as missing_reserved_method_error:
        return missing_reserved_method_error, StageTypes.ERROR

    except ReservedMethodError as reserved_method_error:
        return reserved_method_error, StageTypes.ERROR

    except HookValidationError as hook_validation_error:
        return hook_validation_error, StageTypes.ERROR
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR
    
    else:
        return None, StageTypes.SETUP


async def validate_to_wait_transition(current_stage: Stage, next_stage: Stage):

    try:

        if current_stage.state == StageStates.INITIALIZED:
            current_stage.state = StageStates.VALIDATING
            current_stage.stages = current_stage.context.stages
            
            if current_stage.timeout:
                await asyncio.wait_for(current_stage.run(), timeout=current_stage.timeout)

            else:
                await current_stage.run()
            
            current_stage.state = StageStates.VALIDATED

        next_stage.state = StageStates.VALIDATED
        next_stage.context = current_stage.context

    except MissingReservedMethodError as missing_reserved_method_error:
        return missing_reserved_method_error, StageTypes.ERROR

    except ReservedMethodError as reserved_method_error:
        return reserved_method_error, StageTypes.ERROR

    except HookValidationError as hook_validation_error:
        return hook_validation_error, StageTypes.ERROR
            
    except asyncio.TimeoutError:
        return StageTimeoutError(current_stage), StageTypes.ERROR

    except Exception as stage_execution_error:
        return StageExecutionError(current_stage, next_stage, str(stage_execution_error)), StageTypes.ERROR
    
    else:
        return None, StageTypes.WAIT