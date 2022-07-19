from hedra.core.pipelines.simple_context import SimpleContext
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def setup_to_optimize_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.context
    return None, StageTypes.OPTIMIZE

async def setup_to_execute_transition(current_stage: Stage, next_stage: Stage):

    execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE).items()
    paths = current_stage.context.paths.get(current_stage.name)
    visited = current_stage.context.visited

    execute_stages = {
        stage_name: stage for stage_name, stage in execute_stages if stage_name in paths and stage_name not in visited
    }
  
    current_stage.stages = execute_stages

    setup_stages = await current_stage.run()

    current_stage.context.stages[StageTypes.EXECUTE].update(setup_stages)
    current_stage.context.reporting_config = current_stage.reporting_config

    next_stage.context = current_stage.context

    return None, StageTypes.EXECUTE

async def setup_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.context
    return None, StageTypes.CHECKPOINT
