import functools
from typing import Tuple
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.registry.registrar import registrar
from .hook_type import HookType


@registrar(HookType.CHANNEL)
def channel(*names: Tuple[str, ...], order: int=1):
    
    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper