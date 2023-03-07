from typing import List, Union, Dict, Any, Type, Callable, Awaitable, Tuple
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook
from .hook_metadata import HookMetadata


class TaskHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Tuple[str, ...],
        weight: int=1, 
        order: int=1, 
        metadata: Dict[str, Union[str, int]]={}
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            hook_type=HookType.TASK
        )

        self.names = list(set(names))
        self.call: Type[self._call] = self._call
        self.session: Any = None
        self.action: Any = None
        self.order = order
        self.before: List[Any] = []
        self.after: List[Any] = []
        self.is_notifier = False
        self.is_listener = False
        self.checks: List[Any] = []
        self.channels: List[Any] = []
        self.notifiers: List[Any] = []
        self.listeners: List[Any] = []
        self.metadata = HookMetadata(
            weight=weight,
            order=order,
            **metadata
        )

    def copy(self):
        task_hook = TaskHook(
            self.name,
            self.shortname,
            self._call,
            weight=self.metadata.weight,
            order=self.metadata.order,
            metadata={
                **self.metadata.copy()
            }
        )

        task_hook.checks = list(self.checks)
        task_hook.stage = self.stage

        return task_hook