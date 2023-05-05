import functools
from typing import Optional
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from .validator import MetricHookValidator




@registrar(HookType.METRIC)
def metric(
    metric_type: str, 
    group: Optional[str]='user_metrics',
    order: int=1,
    skip: bool=False
):
    
    MetricHookValidator(
        metric_type=metric_type,
        group=group,
        order=order,
        skip=skip
    )
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper