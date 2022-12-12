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
from hedra.core.graphs.stages.stage import Stage
from hedra.core.graphs import Graph
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


def run_graph(path: str, cpus: int, log_level: str, logfiles_directory: str):

    logging_manager.disable(LoggerTypes.DISTRIBUTED)
    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory

    if os.path.exists(logfiles_directory) is False:
        os.mkdir(logfiles_directory)

    elif os.path.isdir(logfiles_directory) is False:
        os.remove(logfiles_directory)
        os.mkdir(logfiles_directory)

    logger = HedraLogger()
    logger.initialize(log_level)

    graph_name = path
    if os.path.isfile(graph_name):
        graph_name = Path(graph_name).stem

    logger['console'].sync.info(f'Loading graph - {graph_name.capitalize()}.')

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_graphs = hedra_config.get('graphs', {})

    if hedra_graphs.get(graph_name) is None:
        hedra_graphs[graph_name] = path
        with open(hedra_config_filepath, 'w') as hedra_config_file:
            hedra_config['graphs'] = hedra_graphs

            json.dump(hedra_config)   

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

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    pipeline = Graph(
        list(discovered.values()),
        cpus=cpus
    )

    logger['console'].sync.info(f'Validating graph - {graph_name.capitalize()}.')
    pipeline.validate()

    logger['console'].sync.info(f'Running graph - {graph_name.capitalize()}.')
    loop.run_until_complete(pipeline.run())

    os._exit(0)