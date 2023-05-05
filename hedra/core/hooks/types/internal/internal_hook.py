from hedra.core.hooks.types.base.hook_type import HookType
from typing import Callable, Awaitable, Any
from hedra.core.hooks.types.base.hook import Hook


class InternallHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        stage: str = None, 
        hook_type: HookType=HookType.INTERNAL
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            stage=stage, 
            hook_type=hook_type
        )
