import functools
from typing import Dict, Union, Tuple
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.registrar import registrar
from .validator import TaskHookValidator


@registrar(HookType.TASK)
def task(
    *names: Tuple[str, ...],
    weight: int=1, 
    order: int=1, 
    skip: bool=False,
    metadata: Dict[str, Union[str, int]]={}
):
    
    TaskHookValidator(
        names=names,
        weight=weight,
        order=order,
        skip=skip,
        metadata=metadata
    )

    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):

            return func(*args, **kwargs)
                
        return decorator

    return wrapper