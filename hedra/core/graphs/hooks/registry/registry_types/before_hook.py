from typing import List, Type, Callable, Awaitable, Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class BeforeHook(Hook):

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
            hook_type=HookType.BEFORE
        )

        self.call: Type[self._call] = self._call
        self.names = list(set(names))