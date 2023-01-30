from typing import List, Type, Callable, Awaitable, Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ChannelHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]]
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CHANNEL
        )

        self.notifiers: List[str] = []
        self.listeners: List[str] = []

    async def call(self, **kwargs):
        result = await super().call(**{name: value for name, value in kwargs.items() if name in self.params})

        if isinstance(result, dict):
            return {
                **kwargs,
                **result
            }

        notifier_action, listener_actions = result

        return {
            **kwargs,
            'notifier_action': notifier_action,
            'listener_actions': listener_actions
        }