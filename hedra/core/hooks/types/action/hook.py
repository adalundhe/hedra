from typing import (
    List, 
    Union, 
    Dict, 
    Any, 
    Callable, 
    Awaitable, 
    Tuple,
    Optional
)
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_metadata import HookMetadata
from .validator import ActionLoaderConfigValidator


class ActionHook(Hook):

    def __init__(
        self, 
        name: str, 
        shortname: str, 
        call: Callable[..., Awaitable[Any]], 
        *names: Tuple[str, ...],
        weight: int=1, 
        order: int=1, 
        skip: bool=False,
        loader_config: Optional[Dict[str, Any]]=None,
        metadata: Dict[str, Union[str, int]]={}
    ) -> None:
        super().__init__(
            name, 
            shortname, 
            call, 
            skip=skip,
            order=order,
            hook_type=HookType.ACTION,
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
        self.metadata = HookMetadata(
            weight=weight,
            order=order,
            **metadata
        )
        self.loader_config = ActionLoaderConfigValidator(**loader_config)

    def copy(self):
        action_hook = ActionHook(
            self.name,
            self.shortname,
            self._call,
            weight=self.metadata.weight,
            order=self.order,
            skip=self.skip,
            sourcefile=self.sourcefile,
            metadata={
                **self.metadata.copy()
            }
        )

        action_hook.checks = list(self.checks)
        action_hook.stage = self.stage

        return action_hook

    async def call(self, *args, **kwargs):
        return await self._call(*args, **kwargs)
    
    def serialize_action(self) -> str:
        if self.action is None:
            raise Exception('Cannot seralize Action hook without action.')
        
        return self.action.serialize()
        
        