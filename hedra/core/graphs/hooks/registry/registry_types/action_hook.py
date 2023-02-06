from typing import Coroutine, List, Union, Dict, Any, Type, Callable, Awaitable, Tuple
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from .hook import Hook
from .hook_metadata import HookMetadata

class ActionHook(Hook):

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
            hook_type=HookType.ACTION
        )
        
        self.names = list(set(names))
        self.session: Any = None
        self.action: Any = None
        self.checks = []
        self.before: List[str] = []
        self.after: List[str] = []
        self.is_notifier = False
        self.is_listener = False
        self.channels: List[Any] = []
        self.notifiers: List[Any] = []
        self.listeners: List[Any] = []
        self.order = order
        self.metadata = HookMetadata(
            weight=weight,
            order=order,
            **metadata
        )

    def copy(self):
        return ActionHook(
            self.name,
            self.shortname,
            self._call,
            weight=self.metadata.weight,
            order=self.metadata.order,
            checks=self.checks,
            notify=self.notifiers,
            listen=self.listeners
        )

    async def call(self, *args, **kwargs):
        return await self._call(*args, **kwargs)
