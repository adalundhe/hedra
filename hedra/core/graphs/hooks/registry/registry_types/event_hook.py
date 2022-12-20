from typing import Coroutine, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class EventHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        *names: str,
        pre: bool=False
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.EVENT
        )

        self.names = list(set(names))
        self.pre = pre
        self.events: Dict[str, Coroutine] = {}