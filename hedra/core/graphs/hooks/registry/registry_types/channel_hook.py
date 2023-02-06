from typing import List, Type, Callable, Awaitable, Any, Tuple
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ChannelHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]],
        *names: Tuple[str, ...],
        order: int=1
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CHANNEL
        )

        self.notifiers: List[Any] = []
        self.listeners: List[Any] = []
        self.names = list(set(names))
        self.order = order

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
        return ChannelHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            order=self.order
        )