import functools
from .types import HookType
from hedra.test.registry.registrar import registar

@registar(HookType.AFTER_BATCH)
def after_batch(*names):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper