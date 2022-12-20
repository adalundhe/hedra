from typing import Coroutine, Dict, Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class ContextHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        key: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.CONTEXT
        )

        self.context_key = key