import importlib
import ntpath
import sys
import asyncio
import inspect
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List
from hedra.logging import HedraLogger
from hedra.core.hooks.types.event.event import Event, EventHook
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.hooks.types.base.hook import Hook
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


def set_stage_hooks(stage: Stage, generated_hooks: Dict[str, Hook]) -> Stage:
    methods = inspect.getmembers(stage, predicate=inspect.ismethod) 

    for _, method in methods:
        method_name = method.__qualname__
        hook_set: List[Hook] = registrar.all.get(method_name, [])

        for hook in hook_set:
                

            if generated_hooks.get(hook) is None:

                hook._call = hook._call.__get__(stage, stage.__class__)
                setattr(stage, hook.shortname, hook._call)

                if inspect.ismethod(hook.call) is False:
                    hook.call = hook.call.__get__(stage, stage.__class__)
                    setattr(stage, hook.shortname, hook.call)

                generated_hooks[hook] = 'created'
                hook.stage = stage.name
                
                hook.stage_instance: Stage = stage

                hook.name = f'{hook.stage}.{hook.shortname}'
                stage.hooks[hook.hook_type].append(hook)
            

            elif generated_hooks.get(hook) == 'created':

                copied_hook = hook.copy()
    
                

                stage_config: Dict[str, Any]  = stage.to_copy_dict()
                copied_stage = type(stage)()

                for copied_attribute_name, copied_attribute_value in stage_config.items():
                    if inspect.ismethod(copied_attribute_value) is False:
                        setattr(copied_stage, copied_attribute_name, copied_attribute_value)
                        
                copied_hook.stage = stage.name
                copied_hook.stage_instance: Stage = copied_stage
                copied_hook.name = f'{stage.name}.{hook.shortname}'
                copied_hook._call = method

                copied_hook.name = f'{copied_hook.stage}.{copied_hook.shortname}'

                copied_hook._call = copied_hook._call.__get__(stage, stage.__class__)
                setattr(stage, copied_hook.shortname, copied_hook._call)

                if inspect.ismethod(copied_hook.call) is False:
                    copied_hook.call = copied_hook.call.__get__(stage, stage.__class__)
                    setattr(stage, copied_hook.shortname, copied_hook.call)

                if copied_hook not in stage.hooks[hook.hook_type]:
                    stage.hooks[hook.hook_type].append(copied_hook)

    return stage
