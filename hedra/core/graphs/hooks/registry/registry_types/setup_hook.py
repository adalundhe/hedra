from typing import Coroutine, Any, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook
from .hook_metadata import HookMetadata


class SetupHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        metadata: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.SETUP
        )

        self.metadata = HookMetadata(**metadata)