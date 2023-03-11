import functools
from typing import Tuple
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from .validator import LoadHookValidator


@registrar(HookType.LOAD)
def load(*names: Tuple[str, ...], load_path: str=None, order: int=1):
    
    LoadHookValidator(
        names=names,
        load_path=load_path,
        order=order
    )
    
    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper