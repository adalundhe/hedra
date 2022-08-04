from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def validate_to_setup_transition(current_stage: Stage, next_stage: Stage):
    current_stage.stages = current_stage.context.stages
    await current_stage.run()
    next_stage.context = current_stage.context
    return None, StageTypes.SETUP