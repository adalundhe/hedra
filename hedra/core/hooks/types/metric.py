import functools
from typing import Any, Optional
from .types import HookType
from hedra.core.hooks.registry.registrar import registar


@registar(HookType.METRIC)
def metric(group: Optional[str]='user_metrics'):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper