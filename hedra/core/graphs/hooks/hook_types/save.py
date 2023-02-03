import functools
from typing import Tuple
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.SAVE)
def save(
    *names: Tuple[str, ...], 
    save_path: str=None,
    order: int = 1
):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper