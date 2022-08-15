import asyncio
from typing import List
import dill
from hedra.core.engines.client.config import Config
from hedra.core.pipelines.hooks.registry.registrar import registrar
from ...hooks.types.hook import Hook
from ...hooks.types.types import HookType
from hedra.core.pipelines.stages.optimizers import Optimizer
from hedra.core.personas import get_persona


def optimize_stage(serialized_config: str):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    optimization_config = dill.loads(serialized_config)

    execute_stage_name = optimization_config.get('execute_stage_name')
    execute_stage_generation_count = optimization_config.get('execute_stage_generation_count')
    execute_stage_id = optimization_config.get('execute_stage_id')
    execute_stage_config: Config = optimization_config.get('execute_stage_config')
    optimize_iterations = optimization_config.get('optimizer_iterations')
    optimizer_type = optimization_config.get('optimizer_type')
    time_limit = optimization_config.get('time_limit')

    execute_stage_hooks = [
        registrar.all.get(hook_name) for hook_name in optimization_config.get('execute_stage_hooks')
    ]

    batch_size = execute_stage_config.batch_size

    if execute_stage_generation_count > 1 and execute_stage_id == execute_stage_generation_count:
        batch_size = int(batch_size/execute_stage_generation_count) + batch_size%execute_stage_generation_count

    else:
        batch_size = int(batch_size/execute_stage_generation_count)

    execute_stage_config.batch_size = batch_size

    persona = get_persona(execute_stage_config)
    persona.setup({
        HookType.ACTION: execute_stage_hooks
    })

    
    optimizer = Optimizer({
        'iterations': optimize_iterations,
        'algorithm': optimizer_type,
        'persona': persona,
        'time_limit': time_limit
    })

    results = optimizer.optimize(loop)
    optimized_batch_size = results.get('optimized_batch_size')

    execute_stage_config.batch_size = optimized_batch_size

    loop.stop()
    loop.close()

    return {
        'stage': execute_stage_name,
        'config': execute_stage_config,
        'params': results
    }