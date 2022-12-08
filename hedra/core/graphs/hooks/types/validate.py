import functools
from .hook_types import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.VALIDATE)
def validate(stage: str, *names):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper