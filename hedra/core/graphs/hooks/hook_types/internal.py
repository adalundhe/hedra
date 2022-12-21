from typing import Callable
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.registry.registry_types import InternallHook


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