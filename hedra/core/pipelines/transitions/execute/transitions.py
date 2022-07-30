from hedra.core.pipelines.simple_context import SimpleContext
from hedra.core.pipelines.stages.stage import Stage
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes


async def execute_transition(current_stage: Stage, next_stage: Stage):
    results = await current_stage.run()

    current_stage.context.results[current_stage.name] = {
        'results': results,
        'total_elapsed': current_stage.persona.total_elapsed
    }
    current_stage.context.results_stages.append(current_stage)
    current_stage.context.visited.append(current_stage.name)
    
    next_stage.context = current_stage.context

    return current_stage, next_stage


async def execute_to_setup_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.SETUP,
        StageStates.OPTIMIZED
    ]

    if current_stage.state in valid_states:
        current_stage.state = StageStates.EXECUTING

        current_stage, next_stage = await execute_transition(current_stage, next_stage)
        execute_stages = list(current_stage.context.stages.get(StageTypes.EXECUTE).values())
        visited = current_stage.context.visited

        for stage in execute_stages:
            if stage.name not in visited and stage.state == StageStates.SETUP:
                stage.state = StageStates.INITIALIZED

        current_stage.state = StageStates.EXECUTED

    return None, StageTypes.SETUP


async def execute_to_execute_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.SETUP,
        StageStates.OPTIMIZED
    ]

    if current_stage.state in valid_states:
        current_stage.state = StageStates.EXECUTING

        current_stage, next_stage = await execute_transition(current_stage, next_stage)

        current_stage.state = StageStates.EXECUTED

    return None, StageTypes.EXECUTE


async def execute_to_optimize_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.SETUP,
        StageStates.OPTIMIZED
    ]

    if current_stage.state in valid_states:
        current_stage.state = StageStates.EXECUTING

        current_stage, next_stage = await execute_transition(current_stage, next_stage)

        current_stage.state = StageStates.EXECUTED

    return None, StageTypes.OPTIMIZE


async def execute_to_teardown_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.SETUP,
        StageStates.OPTIMIZED
    ]

    if current_stage.state in valid_states:
        current_stage.state = StageStates.EXECUTING

        current_stage, next_stage = await execute_transition(current_stage, next_stage)

        current_stage.state = StageStates.EXECUTED

    return None, StageTypes.TEARDOWN


async def execute_to_analyze_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.SETUP,
        StageStates.OPTIMIZED
    ]

    if current_stage.state in valid_states:
        current_stage.state = StageStates.EXECUTING

        current_stage, next_stage = await execute_transition(current_stage, next_stage)

        current_stage.state = StageStates.EXECUTED

    return None, StageTypes.ANALYZE


async def execute_to_checkpoint_transition(current_stage: Stage, next_stage: Stage):

    valid_states = [
        StageStates.SETUP,
        StageStates.OPTIMIZED
    ]

    if current_stage.state in valid_states:
        current_stage.state = StageStates.EXECUTING

        current_stage, next_stage = await execute_transition(current_stage, next_stage)
        next_stage.data = [
            result.to_dict() for result in current_stage.context.results[current_stage.name]
        ]

        next_stage.previous_stage = current_stage.name

        current_stage.state = StageStates.EXECUTED

    return None, StageTypes.CHECKPOINT
