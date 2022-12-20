from typing import Coroutine, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class RestoreHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        key: str=None,
        restore_filepath: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.RESTORE
        )

        self.context_key = key
        self.restore_path = restore_filepath