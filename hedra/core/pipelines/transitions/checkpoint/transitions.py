from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def checkpoint_to_setup_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED
        
    next_stage.context = current_stage.context
    return None, StageTypes.SETUP


async def checkpoint_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED

    next_stage.context = current_stage.context
    return None, StageTypes.OPTIMIZE


async def checkpoint_to_execute_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED

    next_stage.context = current_stage.context
    return None, StageTypes.EXECUTE


async def checkpoint_to_teardown_transition(current_stage: Stage, next_stage: Stage):
    
    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED

    next_stage.context = current_stage.context
    return None, StageTypes.TEARDOWN


async def checkpoint_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED

    next_stage.context = current_stage.context
    return None, StageTypes.ANALYZE


async def checkpoint_to_submit_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED
    
    next_stage.context = current_stage.context
    return None, StageTypes.SUBMIT
    

async def checkpoint_to_complete_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING

        await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED
    
    next_stage.context = current_stage.context
    return None, StageTypes.COMPLETE

