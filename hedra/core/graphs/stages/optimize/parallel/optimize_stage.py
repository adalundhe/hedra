import json
import dill
import threading
import os
import importlib
import inspect
import ntpath
import sys
from collections import defaultdict
from typing import Any, Dict, List, Union
from pathlib import Path
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.stages.optimize.optimization import Optimizer
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.engines.types.registry import registered_engines
from hedra.core.personas.persona_registry import registered_personas
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.stages.base.import_tools import import_stages, import_plugins
from hedra.core.graphs.stages.optimize.optimization.algorithms import registered_algorithms
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
    graph_path: str= optimization_config.get('graph_path')
    graph_id: str = optimization_config.get('graph_id')
    source_stage_name: str = optimization_config.get('source_stage_name')
    source_stage_id: str = optimization_config.get('source_stage_id')
    execute_stage_name: str = optimization_config.get('execute_stage_name')
    execute_stage_config: Config = optimization_config.get('execute_stage_config')
    execute_stage_plugins: Dict[PluginType, List[str]] = optimization_config.get('execute_stage_plugins')
    optimize_params: List[str] = optimization_config.get('optimize_params')
    optimize_iterations: int = optimization_config.get('optimizer_iterations')
    optimizer_algorithm: str = optimization_config.get('optimizer_algorithm')
    time_limit: int = optimization_config.get('time_limit')
    batch_size: int = optimization_config.get('execute_stage_batch_size')

    metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    discovered = import_stages(graph_path)

    execute_stage: Stage = discovered.get(execute_stage_name)()


    execute_stage_actions = defaultdict(list)
    for hook_name in optimization_config.get('execute_stage_hooks'):
        hook = registrar.all.get(hook_name)

        hook._call = hook._call.__get__(execute_stage, execute_stage.__class__)
        setattr(execute_stage, hook.shortname, hook._call)

        if inspect.ismethod(hook.call) is False:
            hook.call = hook.call.__get__(execute_stage, execute_stage.__class__)
            setattr(execute_stage, hook.shortname, hook.call)

        execute_stage_actions[hook.hook_type].append(hook)

    execute_stage.hooks.update(execute_stage_actions)
    execute_stage_config.batch_size = batch_size

    plugins_by_type = import_plugins(graph_path)

    stage_persona_plugins: List[str] = execute_stage_plugins[PluginType.PERSONA]
    persona_plugins: Dict[str, PersonaPlugin] = plugins_by_type[PluginType.PERSONA]

    stage_engine_plugins: List[str] = execute_stage_plugins[PluginType.ENGINE]
    engine_plugins: Dict[str, EnginePlugin] = plugins_by_type[PluginType.ENGINE]
    
    for plugin_name in stage_persona_plugins:
        plugin = persona_plugins.get(plugin_name)
        plugin.name = plugin_name
        registered_personas[plugin_name] = lambda config: plugin(config)
    
    for plugin_name in stage_engine_plugins:
        plugin = engine_plugins.get(plugin_name)
        plugin.name = plugin_name
        registered_engines[plugin_name] = lambda config: plugin(config)

    for plugin_name, plugin in plugins_by_type[PluginType.OPTIMIZER].items():
        registered_algorithms[plugin_name] = plugin

    setup_stage = Setup()
    setup_stage.logger.spinner.logger.log_level = 'critical'
    setup_stage.plugins_by_type = plugins_by_type
    setup_stage.generation_setup_candidates = 1
    setup_stage.stages[execute_stage.name] = execute_stage
    setup_stage.config = execute_stage_config

    stages = loop.run_until_complete(setup_stage.run())
    
    setup_execute_stage: Execute = stages.get(execute_stage_name)

    logger.filesystem.sync['hedra.optimize'].info(f'{metadata_string} - Setting up Optimization')

    optimizer = Optimizer({
        'graph_name': graph_name,
        'graph_id': graph_id,
        'source_stage_name': source_stage_name,
        'source_stage_id': source_stage_id,
        'params': optimize_params,
        'stage_name': execute_stage_name,
        'stage_config': execute_stage_config,
        'stage_hooks': setup_execute_stage.hooks,
        'iterations': optimize_iterations,
        'algorithm': optimizer_algorithm,
        'time_limit': time_limit
    })

    results = optimizer.optimize()

    execute_stage_config.batch_size = results.get('optimized_batch_size', execute_stage_config.batch_size)
    execute_stage_config.batch_interval = results.get('optimized_batch_interval', execute_stage_config.batch_gradient)
    execute_stage_config.batch_gradient = results.get('optimized_batch_gradient', execute_stage_config.batch_gradient)


    logger.filesystem.sync['hedra.optimize'].info(f'{optimizer.metadata_string} - Optimization complete')

    return {
        'stage': execute_stage.name,
        'config': execute_stage_config,
        'params': results
    }