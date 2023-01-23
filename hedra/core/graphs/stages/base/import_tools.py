import importlib
import ntpath
import sys
import inspect
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List
from hedra.logging import HedraLogger
from hedra.core.graphs.events.event import Event, EventHook
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
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


def set_stage_hooks(stage: Stage) -> Stage:
    methods = inspect.getmembers(stage, predicate=inspect.ismethod) 

    for _, method in methods:
        method_name = method.__qualname__
        hook: Hook = registrar.all.get(method_name)
        
        if hook:
            hook._call = hook._call.__get__(stage, stage.__class__)
            setattr(stage, hook.shortname, hook._call)

            if inspect.ismethod(hook.call) is False:
                hook.call = hook.call.__get__(stage, stage.__class__)
                setattr(stage, hook.shortname, hook.call)

            hook.stage = stage.name
            hook.stage_instance: Stage = stage
            
            stage.hooks[hook.hook_type].append(hook)

    return stage


def set_events(event_hooks: List[EventHook], logging: HedraLogger, metadata_string: str) -> List[EventHook]:

    for event_hook in event_hooks:
        for target_hook_name in event_hook.names:    
            target_hook = registrar.all.get(target_hook_name)

            if target_hook and target_hook.stage_instance:
                logging.filesystem.sync['hedra.core'].info(
                    f'{metadata_string} - Appendng Event - {event_hook.name}:{event_hook.hook_id} - to target Stage - {target_hook.stage}:{target_hook.stage_instance.stage_id} Event Hooks'
                )

                event = Event(target_hook, event_hook)
                event.target_key = event_hook.key

                target_hook_idx = -1

                try:
                    target_hook_names = [hook.name for hook in target_hook.stage_instance.hooks[target_hook.hook_type]]
                    target_hook_idx = target_hook_names.index(target_hook.name)

                except ValueError:
                    pass

                if target_hook_idx >= 0 and isinstance(target_hook, Event):
                    if event_hook.pre is True:
                        target_hook.pre_sources[event_hook.name] = event_hook
                        target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = target_hook

                    else:
                        target_hook.post_sources[event_hook.name] = event_hook
                        target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = target_hook
                    
                    registrar.all[event.name] = target_hook

                elif target_hook_idx >= 0:
                    target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = event
                    registrar.all[event.name] = event
                
                target_hook.stage_instance.linked_events[(target_hook.stage, target_hook.hook_type, target_hook.name)].append(
                    (event_hook.stage, event_hook.hook_type, event_hook.name)
                )

    return event_hooks