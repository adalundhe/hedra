import functools
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.CHECK)
def check(*names, order: int=1):
    
    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper