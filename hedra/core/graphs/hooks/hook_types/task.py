import functools
from typing import Coroutine, Dict, List, Union
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.TASK)
def task(
    weight: int=1, 
    order: int=1, 
    metadata: Dict[str, Union[str, int]]={}, checks: List[Coroutine]=[],
    notify: List[str]=[], 
    listen: List[str]=[]
):

    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):

            return func(*args, **kwargs)
                
        return decorator

    return wrapper