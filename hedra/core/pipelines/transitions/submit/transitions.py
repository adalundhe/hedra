from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def submit_to_setup_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.SUBMITTING
        
        current_stage.summaries = current_stage.context.summaries
        await current_stage.run()

        current_stage.state = StageStates.SUBMITTED
        next_stage.state = StageStates.SUBMITTED
    
    next_stage.context = current_stage.context
    return None, StageTypes.SETUP


async def submit_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.SUBMITTING
        
        current_stage.summaries = current_stage.context.summaries
        await current_stage.run()

        current_stage.state = StageStates.SUBMITTED
        next_stage.state = StageStates.SUBMITTED
    
    next_stage.context = current_stage.context
    return None, StageTypes.OPTIMIZE


async def submit_to_execute_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.SUBMITTING
        
        current_stage.summaries = current_stage.context.summaries
        await current_stage.run()

        current_stage.state = StageStates.SUBMITTED
        next_stage.state = StageStates.SUBMITTED
    
    next_stage.context = current_stage.context
    return None, StageTypes.EXECUTE


async def submit_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.SUBMITTING
        
        current_stage.summaries = current_stage.context.summaries
        await current_stage.run()

        current_stage.state = StageStates.SUBMITTED
        next_stage.state = StageStates.SUBMITTED
    
        next_stage.previous_stage = current_stage.name
        next_stage.context = current_stage.context
        
    return None, StageTypes.CHECKPOINT


async def submit_to_complete_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.SUBMITTING
        
        current_stage.summaries = current_stage.context.summaries
        await current_stage.run()

        current_stage.state = StageStates.SUBMITTED
        next_stage.state = StageStates.SUBMITTED
    
    next_stage.context = current_stage.context
    return None, StageTypes.COMPLETE
