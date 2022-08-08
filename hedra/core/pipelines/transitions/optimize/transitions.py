import asyncio
from typing import Any
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def optimize_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.INITIALIZED,
        StageStates.SETUP
    ]

    if next_stage.state in valid_states:
        next_stage.state = StageStates.OPTIMIZING

        next_stage_name = next_stage.name
        execute_stage = current_stage.context.stages.get(
            StageTypes.EXECUTE
        ).get(next_stage_name)
        
        current_stage.persona = execute_stage.persona

        if current_stage.timeout:
            optimized_persona = await asyncio.wait_for(current_stage.run(), current_stage.timeout)

        else:
            optimized_persona = await current_stage.run()

        next_stage.persona = optimized_persona
        next_stage.context.optimized_params = current_stage.results

        next_stage.state = StageStates.OPTIMIZED

        next_stage.context = current_stage.context


async def optimize_to_execute_transition(current_stage: Stage, next_stage: Stage):
    await optimize_transition(current_stage, next_stage)

    return None, StageTypes.EXECUTE


async def optimize_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):
    await optimize_transition(current_stage, next_stage)

    next_stage.data = next_stage.context.optimized_params
    next_stage.previous_stage = current_stage.name

    return None, StageTypes.CHECKPOINT
