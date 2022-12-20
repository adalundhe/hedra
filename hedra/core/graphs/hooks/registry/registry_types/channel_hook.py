from typing import Coroutine, List
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ChannelHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CHANNEL
        )

        self.notifiers: List[str] = []
        self.listeners: List[str] = []