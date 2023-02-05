from typing import Coroutine, Dict, Any, Callable, Awaitable, Type, Optional, Tuple
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class EventHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Optional[Tuple[str, ...]],
        pre: bool=False,
        key: Optional[str]=None,
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.EVENT
        )

        self.names = list(set(names))
        self.pre = pre
        self.key = key
        self.order = order
        self.events: Dict[str, Coroutine] = {}

    async def call(self, **kwargs):
        result = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(result, dict):
            return {
                **kwargs,
                **result
            }

        return {
            **kwargs,
            self.shortname: result
        }

    def copy(self):
        return EventHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            pre=self.pre,
            key=self.key,
            order=self.order
        )