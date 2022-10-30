import asyncio
import dill
from typing import Any, Dict, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.optimization import Optimizer
from hedra.core.personas import get_persona


def optimize_stage(serialized_config: str):

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    optimization_config: Dict[str, Union[str, int, Any]] = dill.loads(serialized_config)

    execute_stage_name: str = optimization_config.get('execute_stage_name')
    execute_stage_config: Config = optimization_config.get('execute_stage_config')
    optimize_params: List[str] = optimization_config.get('optimize_params')
    optimize_iterations: int = optimization_config.get('optimizer_iterations')
    optimizer_algorithm: str = optimization_config.get('optimizer_algorithm')
    time_limit: int = optimization_config.get('time_limit')
    batch_size: int = optimization_config.get('execute_stage_batch_size')

    execute_stage_hooks = [
        registrar.all.get(hook_name) for hook_name in optimization_config.get('execute_stage_hooks')
    ]

    execute_stage_config.batch_size = batch_size

    persona = get_persona(execute_stage_config)
    persona.setup({
        HookType.ACTION: execute_stage_hooks
    })

    optimizer = Optimizer({
        'params': optimize_params,
        'stage_name': execute_stage_name,
        'iterations': optimize_iterations,
        'algorithm': optimizer_algorithm,
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