import functools
from typing import Optional
from hedra.core.graphs.hooks.types.base.hook_type import HookType
from hedra.core.graphs.hooks.types.base.registrar import registrar
from .validator import MetricHookValidator


@registrar(HookType.METRIC)
def metric(group: Optional[str]='user_metrics'):
    
    MetricHookValidator(group=group)
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper