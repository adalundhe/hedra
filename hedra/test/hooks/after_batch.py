import functools
from .types import HookType


def after_batch(*names):
    
    def wrapper(func):
        func.names = names
        func.is_action = True
        func.hook_type = HookType.AFTER_BATCH

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper