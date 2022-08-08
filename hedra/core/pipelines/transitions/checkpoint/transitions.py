import asyncio
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes

async def checkpoint_transition(current_stage: Stage, next_stage: Stage):
    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.CHECKPOINTING
        
        if current_stage.timeout:
            await asyncio.wait_for(current_stage.run(), current_stage.timeout)
        
        else:
            await current_stage.run()

        current_stage.state = StageStates.CHECKPOINTED

    next_stage.context = current_stage.context


async def checkpoint_to_setup_transition(current_stage: Stage, next_stage: Stage):

    await checkpoint_transition(current_stage, next_stage)
        
    return None, StageTypes.SETUP


async def checkpoint_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    await checkpoint_transition(current_stage, next_stage)

    return None, StageTypes.OPTIMIZE


async def checkpoint_to_execute_transition(current_stage: Stage, next_stage: Stage):

    await checkpoint_transition(current_stage, next_stage)

    return None, StageTypes.EXECUTE


async def checkpoint_to_teardown_transition(current_stage: Stage, next_stage: Stage):
    
    await checkpoint_transition(current_stage, next_stage)

    return None, StageTypes.TEARDOWN


async def checkpoint_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    await checkpoint_transition(current_stage, next_stage)

    return None, StageTypes.ANALYZE


async def checkpoint_to_submit_transition(current_stage: Stage, next_stage: Stage):

    await checkpoint_transition(current_stage, next_stage)

    return None, StageTypes.SUBMIT
    

async def checkpoint_to_complete_transition(current_stage: Stage, next_stage: Stage):

    await checkpoint_transition(current_stage, next_stage)

    return None, StageTypes.COMPLETE

