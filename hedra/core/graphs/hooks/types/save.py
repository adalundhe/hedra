import functools
from .hook_types import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.SAVE)
def save(checkpoint_filepath: str):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper