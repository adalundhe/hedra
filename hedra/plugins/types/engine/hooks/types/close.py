import functools
from typing import Coroutine, Dict, List, Union
from .types import PluginHooks
from .plugin_hook import PluginHook
from hedra.plugins.types.engine.hooks.registry.registrar import plugin_registrar


@plugin_registrar(PluginHooks.ON_CLOSE)
def close(weight: int=1, order: int=1, metadata: Dict[str, Union[str, int]]={}, checks: List[Coroutine]=[]):

    def wrapper(func) -> PluginHook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):

            return func(*args, **kwargs)
                
        return decorator

    return wrapper