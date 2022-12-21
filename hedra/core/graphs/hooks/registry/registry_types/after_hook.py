from typing import Any, List, Type, Callable, Awaitable
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class AfterHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: List[str]
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.AFTER
        )

        self.call: Type[self._call] = self._call
        self.names = list(set(names))


