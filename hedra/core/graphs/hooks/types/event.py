import functools
from .hook_types import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.EVENT)
def event(*names):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper