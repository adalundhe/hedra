import functools
from typing import Any, Optional
from .types import HookType
from hedra.core.pipelines.hooks.registry.registrar import registrar


@registrar(HookType.METRIC)
def metric(group: Optional[str]='user_metrics'):
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper