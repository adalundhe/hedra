import functools
from .types import HookType
from hedra.core.pipelines.hooks.registry.registrar import registar


@registar(HookType.BEFORE)
def before(*names):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper