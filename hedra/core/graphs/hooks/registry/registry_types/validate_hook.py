from typing import Coroutine, List
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ValidateHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        *names: str
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.VALIDATE
        )

        self.names = list(set(names))