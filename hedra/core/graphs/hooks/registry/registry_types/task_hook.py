from typing import Coroutine, List, Union, Dict, Any
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook
from .hook_metadata import HookMetadata


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
            hook_type=HookType.TASK
        )

        self.session: Any = None
        self.action: Any = None
        self.notiy = notify
        self.listen = listen
        self.checks = checks
        self.is_notifier = len(notify) > 0
        self.is_listener = len(listen) > 0
        self.notifiers: List[str] = []
        self.listeners: List[str] = []
        self.metadata = HookMetadata(
            weight=weight,
            order=order,
            **metadata
        )