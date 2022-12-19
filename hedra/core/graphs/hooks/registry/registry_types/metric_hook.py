from typing import Coroutine, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook, Metadata

class MetricHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        group: Optional[str] = None
    ) -> None:
        
        if names is None:
            names = []

        elif isinstance(names, str):
            names = [names]

        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.METRIC, 
            metadata=Metadata(), 
            group=group
        )