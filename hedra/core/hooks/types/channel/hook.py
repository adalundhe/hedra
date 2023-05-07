from typing import List, Callable, Awaitable, Any, Tuple
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook


class ChannelHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]],
        *names: Tuple[str, ...],
        order: int=1,
        skip: bool=False
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            skip=skip,
            order=order,
            hook_type=HookType.CHANNEL,
        )

        self.notifiers: List[Any] = []
        self.listeners: List[Any] = []
        self.names = list(set(names))

    async def call(self, **kwargs):

        if self.skip:
            return kwargs

        result = await super().call(**{
            name: value for name, value in kwargs.items() if name in self.params
        })

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
        channel_hook = ChannelHook(
            self.name,
            self.shortname,
            self._call,
            *self.names,
            order=self.order,
            skip=self.skip,
        )

        channel_hook.stage = self.stage

        return channel_hook