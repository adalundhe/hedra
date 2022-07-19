from operator import ne
from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def analyze_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    if current_stage.state == StageStates.INITIALIZED:
        raw_results = current_stage.context.results
        execute_stages = current_stage.context.stages.get(StageTypes.EXECUTE)
        paths = current_stage.context.paths

        results_to_calculate = {}
        for stage_name in raw_results.keys():
            stage = execute_stages.get(stage_name)

            if stage.state == StageStates.EXECUTED and current_stage.name in paths.get(stage.name):
                stage.state = StageStates.ANALYZING
                results_to_calculate[stage_name] = raw_results.get(stage_name)
        
        print(list(results_to_calculate.keys()))

        current_stage.raw_results = results_to_calculate
        summary = await current_stage.run()
        current_stage.context.summaries.update(summary)
        next_stage.context = current_stage.context

    return None, StageTypes.CHECKPOINT
