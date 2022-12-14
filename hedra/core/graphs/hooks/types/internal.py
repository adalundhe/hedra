from typing import Callable
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar
from .hook import Hook


class Internal:
    is_internal = True

    def __init__(self) -> None:
        pass

    def __call__(self, call: Callable=None) -> Hook:

        hook_fullname = call.__qualname__
        stage_name, hook_shortname = hook_fullname.split('.')

        hook = Hook(
            hook_fullname,
            call.__name__,
            call,
            hook_type=HookType.INTERNAL
        )

        registrar.reserved[stage_name][hook_shortname] = hook

        return call