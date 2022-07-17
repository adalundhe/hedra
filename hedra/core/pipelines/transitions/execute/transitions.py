from types import SimpleNamespace
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def execute_to_execute_transition(current_stage: Stage, next_stage: Stage):

    stage = current_stage.context.stages[StageTypes.EXECUTE].get(next_stage.name)
    next_stage.context = SimpleNamespace(
        persona=stage.persona
    )

    results = await next_stage.run()

    next_stage.context = current_stage.context
    next_stage.context.results = {
        next_stage.name: results
    }

    return None, StageTypes.EXECUTE

async def execute_to_optimize_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.context
    return None, StageTypes.OPTIMIZE

async def execute_to_teardown_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.context
    return None, StageTypes.TEARDOWN

async def execute_to_analyze_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.context
    return None, StageTypes.ANALYZE

async def execute_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):
    next_stage.context = current_stage.context
    return None, StageTypes.CHECKPOINT
