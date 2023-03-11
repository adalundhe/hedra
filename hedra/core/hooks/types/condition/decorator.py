import functools
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from .validator import ConditionHookValidator


@registrar(HookType.CONDITION)
def condition(
    *names,
    order: int=1
):
    ConditionHookValidator(
        names=names,
        order=order
    )

    def wrapper(func):

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)

        return decorator

    return wrapper