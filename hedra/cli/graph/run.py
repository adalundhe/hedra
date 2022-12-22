import asyncio
import os
import inspect
import uvloop
import json
uvloop.install()
import sys
import importlib
import ntpath
from pathlib import Path
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs import Graph
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


def run_graph(
    path: str, 
    cpus: int, 
    log_level: str, 
    logfiles_directory: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
):

    if logfiles_directory is None:
        logfiles_directory = os.getcwd()

    logging_manager.disable(
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory

    if os.path.exists(logfiles_directory) is False:
        os.mkdir(logfiles_directory)

    elif os.path.isdir(logfiles_directory) is False:
        os.remove(logfiles_directory)
        os.mkdir(logfiles_directory)

    logger = HedraLogger()
    logger.initialize()

    graph_name = path
    if os.path.isfile(graph_name):
        graph_name = Path(graph_name).stem

    logger['console'].sync.info(f'Loading graph - {graph_name.capitalize()}\n')

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_graphs = hedra_config.get('graphs', {})
    hedra_core_config = hedra_config.get('core', {
        'connection_validation_retries': 3
    })

    hedra_core_config['bypass_connection_validation'] = bypass_connection_validation

    if connection_validation_retries:
        hedra_core_config['connection_validation_retries'] = connection_validation_retries
    
    if path in hedra_graphs:
        path = hedra_graphs.get(path)
    
    package_dir = Path(path).resolve().parent
    package_dir_path = str(package_dir)
    package_dir_module = package_dir_path.split('/')[-1]
    
    package = ntpath.basename(path)
    package_slug = package.split('.')[0]
    spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', path)
    
    if path not in sys.path:
        sys.path.append(str(package_dir.parent))

    module = importlib.util.module_from_spec(spec)

    sys.modules[module.__name__] = module

    spec.loader.exec_module(module)
    
    direct_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())

    discovered = {}
    for name, stage_candidate in inspect.getmembers(module):
        if inspect.isclass(stage_candidate) and issubclass(stage_candidate, Stage) and stage_candidate not in direct_decendants:
            discovered[name] = stage_candidate

    if hedra_graphs.get(graph_name) is None:
        hedra_graphs[graph_name] = module.__file__
        with open(hedra_config_filepath, 'w') as hedra_config_file:
            hedra_config['graphs'] = hedra_graphs
            json.dump(hedra_config, hedra_config_file, indent=4)   

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    logger.filesystem.sync.create_logfile('hedra.core.log')
    logger.filesystem.create_filelogger('hedra.core.log')

    graph = Graph(
        graph_name,
        list(discovered.values()),
        config={
            **hedra_core_config,
            'graph_module': module.__name__
        },
        cpus=cpus
    )

    graph.assemble()

    loop.run_until_complete(graph.run())

    logger.filesystem.sync['hedra.core'].info(f'{graph.metadata_string} - Completed - {graph.logger.spinner.display.total_timer.elapsed_message}\n')
    logger.console.sync.info(f'\nGraph - {graph_name.capitalize()} - completed! {graph.logger.spinner.display.total_timer.elapsed_message}\n')

    os._exit(0)