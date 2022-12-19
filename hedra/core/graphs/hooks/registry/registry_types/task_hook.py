from typing import Coroutine, List, Union, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook, Metadata

class TaskHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Coroutine, 
        weight: int=1, 
        order: int=1, 
        metadata: Dict[str, Union[str, int]]={}, 
        checks: List[Coroutine]=[],
        notify: List[str]=[],
        listen: List[str]=[]
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.TASK, 
            notify=notify,
            listen=listen,
            checks=checks,
            metadata=Metadata(
                weight=weight,
                order=order,
                **metadata
            )
        )