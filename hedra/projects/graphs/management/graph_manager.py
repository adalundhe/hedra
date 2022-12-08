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
        self.config = config

    def execute_workflow(self, workflow_actions: List[str]):
        
        for workflow_action in workflow_actions:


            action: Union[Initialize, Syncrhonize] = self._actions.get(workflow_action)(
                self.config,
                list(self.discovered_graphs.values())
            )

            if action is None:
                raise InvalidActionError(
                    workflow_action, 
                    list(self._actions.keys())
                )

            action.execute()

    def discover_graph_files(self) -> Dict[str, str]:

        candidate_graph_files = glob.glob(self.config.path + '/**/*.py', recursive=True)

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
            
                stage_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())
                # plugin_decendants = list({cls.__name__: cls for cls in Plugin.__subclasses__()}.values())

                discovered = {}
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, Stage) and obj not in stage_decendants:
                        discovered[name] = obj

                if len(discovered) > 0:               
                    graph_filepath = Path(candidate_graph_file_path)                
                    self.discovered_graphs[graph_filepath.stem] = str(graph_filepath.resolve())

            except Exception:
                pass

        
        return self.discovered_graphs