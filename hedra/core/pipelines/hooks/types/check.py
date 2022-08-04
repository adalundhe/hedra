import functools
from .types import HookType
from .hook import Hook
from hedra.core.pipelines.hooks.registry.registrar import registrar


@registrar(HookType.CHECK)
def check(*names):
    
    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper