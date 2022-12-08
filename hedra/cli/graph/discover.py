import sys
import os
import glob
import inspect
import importlib
import ntpath
import json
from pathlib import Path
from typing import List, Dict
from hedra.core.graphs.stages.stage import Stage


def discover_graphs(path: str):

    candidate_graph_files = glob.glob(path + '/**/*.py', recursive=True)

    discovered_graphs: Dict[str, str] = {}

    for candidate_graph_file_path in candidate_graph_files:

        package_dir = Path(candidate_graph_file_path).resolve().parent
        package_dir_path = str(package_dir)
        package_dir_module = package_dir_path.split('/')[-1]
        
        package = ntpath.basename(candidate_graph_file_path)
        package_slug = package.split('.')[0]
        spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', candidate_graph_file_path)

        if candidate_graph_file_path not in sys.path:
            sys.path.append(str(package_dir.parent))

        module = importlib.util.module_from_spec(spec)
        sys.modules[module.__name__] = module

        try:
            spec.loader.exec_module(module)
            direct_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())

            discovered = {}
            for name, obj in inspect.getmembers(module):
                if inspect.isclass(obj) and issubclass(obj, Stage) and obj not in direct_decendants:
                    discovered[name] = obj

            if len(discovered) > 0:
                graph_filepath = Path(candidate_graph_file_path)                
                discovered_graphs[graph_filepath.stem] = str(graph_filepath.resolve())

        except Exception:
            pass

    hedra_graphfile_path = os.path.join(
        path,
        '.graphs.json'
    )

    
    with open(hedra_graphfile_path, 'w+') as hedra_graphfile:
        hedra_graphs: Dict[str, str] = {}

        try:
            hedra_graphs = json.load(hedra_graphfile)

        except json.JSONDecodeError:
            pass

        hedra_graphs.update(discovered_graphs)

        json.dump(hedra_graphs, hedra_graphfile, indent=4)


        