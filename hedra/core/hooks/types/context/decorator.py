import functools
from typing import Tuple
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from .validator import ContextHookValidator


@registrar(HookType.CONTEXT)
def context(
    *names: Tuple[str], 
    order: int=1
):
    ContextHookValidator(
        names=names,
        order=order
    )

    def wrapper(func) -> Hook:

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper