from typing import Callable
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from .hook import InternallHook


class Internal:
    is_internal = True

    def __init__(self) -> None:
        pass

    def __call__(self, call: Callable=None) -> InternallHook:

        hook_fullname = call.__qualname__
        stage_name, hook_shortname = hook_fullname.split('.')

        hook = InternallHook(
            hook_fullname,
            call.__name__,
            call,
            hook_type=HookType.INTERNAL
        )

        registrar.reserved[stage_name][hook_shortname] = hook

        return call