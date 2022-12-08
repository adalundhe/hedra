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


def run_graph(path: str, cpus: int):

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_graphs = hedra_config.get('graphs', {})
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
    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and issubclass(obj, Stage) and obj not in direct_decendants:
            discovered[name] = obj

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    pipeline = Graph(
        list(discovered.values()),
        cpus=cpus
    )

    pipeline.validate()
    
    loop.run_until_complete(pipeline.run())

    os._exit(0)