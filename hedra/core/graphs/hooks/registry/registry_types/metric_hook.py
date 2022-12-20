from typing import Coroutine, Optional
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook


class MetricHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        group: Optional[str] = None
    ) -> None:        
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.METRIC
        )

        self.group = group