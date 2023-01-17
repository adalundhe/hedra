import importlib
import ntpath
import sys
import inspect
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.common.plugin import Plugin
from .stage import Stage


def import_from_path(path: str) -> Any:
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

    return module


def import_stages(path: str) -> Dict[str, Stage]:
    
    module = import_from_path(path)
    direct_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())

    discovered = {}
    for name, stage_candidate in inspect.getmembers(module):
        if inspect.isclass(stage_candidate) and issubclass(stage_candidate, Stage) and stage_candidate not in direct_decendants:
            discovered[name] = stage_candidate

    return discovered


def import_plugins(path: str) -> Dict[PluginType, Dict[str, Plugin]]:
    module = import_from_path(path)
    direct_decendants = list({cls.__name__: cls for cls in Plugin.__subclasses__()}.values())

    plugins_by_type = defaultdict(dict)
    for name, plugin_candidate in inspect.getmembers(module):
        if inspect.isclass(plugin_candidate) and issubclass(plugin_candidate, Plugin) and plugin_candidate not in direct_decendants:
            plugins_by_type[plugin_candidate.type][plugin_candidate.name] = plugin_candidate

    return plugins_by_type