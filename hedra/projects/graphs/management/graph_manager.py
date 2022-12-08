import sys
import glob
import importlib
import ntpath
import inspect
from pathlib import Path
from typing import List, Union, Dict
from hedra.core.graphs.stages.stage import Stage
from hedra.plugins.types.common.plugin import Plugin
from .actions import (
    Syncrhonize,
    Initialize,
    RepoConfig
)

from .exceptions import InvalidActionError


class GraphManager:

    def __init__(self, config: RepoConfig) -> None:
        self._actions = {
            'initialize': Initialize,
            'synchronize': Syncrhonize   
        }

        self.discovered_graphs: Dict[str, str] = {}
        self.discovered_plugins: Dict[str, str] = {}
        self.config = config

    def execute_workflow(self, workflow_actions: List[str]):
        
        for workflow_action in workflow_actions:

            init_files = glob.glob(self.config.path + '/**/__init__.py', recursive=True)

            discovered = [
                *list(self.discovered_graphs.values()),
                *list(self.discovered_plugins.values()),
                *init_files
            ]
            action: Union[Initialize, Syncrhonize] = self._actions.get(workflow_action)(
                self.config,
                discovered
            )

            if action is None:
                raise InvalidActionError(
                    workflow_action, 
                    list(self._actions.keys())
                )

            action.execute()

    def discover_graph_files(self) -> Dict[str, str]:

        candidate_files = glob.glob(self.config.path + '/**/*.py', recursive=True)

        for candidate_filepath in candidate_files:

            package_dir = Path(candidate_filepath).resolve().parent
            package_dir_path = str(package_dir)
            package_dir_module = package_dir_path.split('/')[-1]
            
            package = ntpath.basename(candidate_filepath)
            package_slug = package.split('.')[0]
            spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', candidate_filepath)

            if candidate_filepath not in sys.path:
                sys.path.append(str(package_dir.parent))

            module = importlib.util.module_from_spec(spec)
            sys.modules[module.__name__] = module

            try:
                spec.loader.exec_module(module)
            
                stage_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())
                plugin_decendants = list({cls.__name__: cls for cls in Plugin.__subclasses__()}.values())

                graphs = {}
                plugins = {}
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, Stage) and obj not in stage_decendants:
                        graphs[name] = obj

                    elif inspect.isclass(obj) and issubclass(obj, Plugin) and obj not in plugin_decendants:
                        plugins[name] = obj


                if len(graphs) > 0:               
                    graph_filepath = Path(candidate_filepath)                
                    self.discovered_graphs[graph_filepath.stem] = str(graph_filepath.resolve())

                if len(plugins) > 0:
                    plugin_filepath = Path(candidate_filepath)
                    self.discovered_plugins[plugin_filepath.stem] = str(plugin_filepath.resolve())

            except Exception:
                pass

        
        return {
            'graphs': self.discovered_graphs,
            'plugins': self.discovered_plugins
        }