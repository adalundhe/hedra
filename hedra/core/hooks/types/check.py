import functools
from .types import HookType
from .hook import Hook
from hedra.core.hooks.registry.registrar import registar


@registar(HookType.CHECK)
def check(*names):
    
    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper