import functools
from typing import Coroutine, Dict, List, Union
from .types import HookType
from .hook import Hook
from hedra.core.pipelines.hooks.registry.registrar import registrar


@registrar(HookType.TASK)
def task(weight: int=1, order: int=1, metadata: Dict[str, Union[str, int]]={}, checks: List[Coroutine]=[]):

    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):

            return func(*args, **kwargs)
                
        return decorator

    return wrapper