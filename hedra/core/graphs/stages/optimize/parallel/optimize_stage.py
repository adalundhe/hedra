import json
import dill
import threading
import os
from collections import defaultdict
from typing import Any, Dict, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.stages.optimize.optimization import Optimizer
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)
from hedra.core.personas import get_persona


def optimize_stage(serialized_config: str):

    import asyncio
    import uvloop
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    logging_config = hedra_config.get('logging', {})
    logfiles_directory = logging_config.get(
        'logfiles_directory',
        os.getcwd()
    )

    log_level = logging_config.get('log_level', 'info')

    logging_manager.disable(
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory


    logger = HedraLogger()
    logger.initialize()
    logger.filesystem.sync.create_logfile('hedra.core.log')
    logger.filesystem.sync.create_logfile('hedra.optimize.log')

    logger.filesystem.create_filelogger('hedra.core.log')
    logger.filesystem.create_filelogger('hedra.optimize.log')

    thread_id = threading.current_thread().ident
    process_id = os.getpid()

    optimization_config: Dict[str, Union[str, int, Any]] = dill.loads(serialized_config)

    graph_name: str = optimization_config.get('graph_name')
    graph_id: str = optimization_config.get('graph_id')
    source_stage_name: str = optimization_config.get('source_stage_name')
    source_stage_id: str = optimization_config.get('source_stage_id')
    execute_stage_name: str = optimization_config.get('execute_stage_name')
    execute_stage_config: Config = optimization_config.get('execute_stage_config')
    optimize_params: List[str] = optimization_config.get('optimize_params')
    optimize_iterations: int = optimization_config.get('optimizer_iterations')
    optimizer_algorithm: str = optimization_config.get('optimizer_algorithm')
    time_limit: int = optimization_config.get('time_limit')
    batch_size: int = optimization_config.get('execute_stage_batch_size')

    metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    execute_stage_hooks = defaultdict(list)
    for hook_name in optimization_config.get('execute_stage_hooks'):
        hook = registrar.all.get(hook_name)
        execute_stage_hooks[hook.hook_type].append(hook)


    logger.filesystem.sync['hedra.optimize'].info(f'{metadata_string} - Setting up Optimization')

    execute_stage_config.batch_size = batch_size

    persona = get_persona(execute_stage_config)
    persona.setup(execute_stage_hooks, metadata_string)

    optimizer = Optimizer({
        'graph_name': graph_name,
        'graph_id': graph_id,
        'source_stage_name': source_stage_name,
        'source_stage_id': source_stage_id,
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

    logger.filesystem.sync['hedra.optimize'].info(f'{optimizer.metadata_string} - Optimization complete')

    return {
        'stage': execute_stage_name,
        'config': execute_stage_config,
        'params': results
    }