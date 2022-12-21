from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from typing import Type, Callable, Awaitable, Any
from .hook import Hook


class InternallHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        stage: str = None, 
        hook_type=HookType.ACTION
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            stage, 
            hook_type
        )

        self.call: Type[self._call] = self._call