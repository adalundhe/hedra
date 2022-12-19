from typing import Coroutine, List, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook, Metadata

class BeforeHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        names: List[str]=[]
    ) -> None:

        if names is None:
            names = []

        elif isinstance(names, str):
            names = [names]

        super().__init__(
            name, 
            shortname, 
            call, 
            names=names,
            hook_type=HookType.BEFORE, 
            metadata=Metadata()
        )