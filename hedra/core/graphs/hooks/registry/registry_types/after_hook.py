from typing import Coroutine, List, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class AfterHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        *names: List[str]
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.AFTER
        )

        self.names = list(set(names))


