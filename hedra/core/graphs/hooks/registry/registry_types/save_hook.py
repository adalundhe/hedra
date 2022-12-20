from typing import Coroutine, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class SaveHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        key: str=None,
        checkpoint_filepath: str=None
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.SAVE
        )

        self.context_key = key
        self.save_path = checkpoint_filepath