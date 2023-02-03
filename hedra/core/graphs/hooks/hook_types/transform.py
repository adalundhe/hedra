import functools
from typing import Optional
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.TRANSFORM)
def transform(
    *names, 
    pre: bool=False,
    order: int=1
):

    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper