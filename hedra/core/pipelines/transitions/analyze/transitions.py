from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def analyze_transition(current_stage: Stage, next_stage: Stage):
    if current_stage.state == StageStates.INITIALIZED:
        current_stage.state = StageStates.ANALYZING

        raw_results = current_stage.context.results
        execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE)
        paths = current_stage.context.paths

        valid_states = [
            StageStates.EXECUTED, 
            StageStates.CHECKPOINTED, 
            StageStates.TEARDOWN_COMPLETE
        ]

        results_to_calculate = {}
        for stage_name in raw_results.keys():
            stage = execute_stages.get(stage_name)

            in_path = current_stage.name in paths.get(stage.name)

            if stage.state in valid_states and in_path:
                stage.state = StageStates.ANALYZING
                results_to_calculate[stage_name] = raw_results.get(stage_name)
        
        current_stage.raw_results = results_to_calculate
        summary = await current_stage.run()
        current_stage.context.summaries.update(summary)

        current_stage.state = StageStates.ANALYZED

        next_stage.context = current_stage.context
        

async def analyze_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    await analyze_transition(current_stage, next_stage)

    next_stage.data = current_stage.context.summaries
    next_stage.previous_stage = current_stage.name

    return None, StageTypes.CHECKPOINT


async def analyze_to_submit_transition(current_stage: Stage, next_stage: Stage):

    await analyze_transition(current_stage, next_stage)

    return None, StageTypes.SUBMIT
