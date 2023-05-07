from typing import (
    Coroutine, 
    Dict, 
    Any, 
    Callable, 
    Awaitable, 
    Optional, 
    Tuple
)
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook


class EventHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Optional[Tuple[str, ...]],
        pre: bool=False,
        key: Optional[str]=None,
        order: int=1,
        skip: bool=False
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            order=order,
            skip=skip,
            hook_type=HookType.EVENT
        )

        self.names = list(set(names))
        self.pre = pre
        self.key = key
        self.events: Dict[str, Coroutine] = {}

    async def call(self, **kwargs):

        if self.skip:
            return kwargs

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
        event_hook = EventHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            pre=self.pre,
            key=self.key,
            order=self.order,
            skip=self.skip,
        )

        event_hook.stage = self.stage

        return event_hook