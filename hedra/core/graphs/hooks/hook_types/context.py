import functools
from typing import Tuple, Optional
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.registry.registrar import registrar
from .hook_type import HookType


@registrar(HookType.CONTEXT)
def context(
    *names: Tuple[str], 
    store: Optional[str]=None, 
    load: Optional[str]=None, 
    pre: bool=False,
    order: int=1
):

    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper