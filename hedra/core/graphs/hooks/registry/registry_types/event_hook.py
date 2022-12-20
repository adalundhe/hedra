from typing import Coroutine, Tuple
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook, Metadata

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
            names=names,
            hook_type=HookType.EVENT, 
            metadata=Metadata(pre=pre)
        )