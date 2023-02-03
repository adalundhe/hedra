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
        metadata: Dict[str, Union[str, int]]={}, 
        checks: List[Callable[..., Awaitable[Any]]]=[],
        notify: List[str]=[],
        listen: List[str]=[]
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
        self.notiy = notify
        self.listen = listen
        self.checks = checks
        self.order = order
        self.before: List[str] = []
        self.after: List[str] = []
        self.is_notifier = len(notify) > 0
        self.is_listener = len(listen) > 0
        self.notifiers: List[str] = notify
        self.listeners: List[str] = listen
        self.metadata = HookMetadata(
            weight=weight,
            order=order,
            **metadata
        )

    def copy(self):
        return TaskHook(
            self.name,
            self.shortname,
            self._call,
            weight=self.metadata.weight,
            order=self.metadata.order,
            checks=self.checks,
            notify=self.notifiers,
            listen=self.listeners
        )