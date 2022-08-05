from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def validate_to_setup_transition(current_stage: Stage, next_stage: Stage):
    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.VALIDATING
        current_stage.stages = current_stage.context.stages
        await current_stage.run()
        
        current_stage.state = StageStates.VALIDATED

    next_stage.state = StageStates.VALIDATED
    next_stage.context = current_stage.context
    return None, StageTypes.SETUP