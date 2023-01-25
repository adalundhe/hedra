from typing import Any, Dict, TypeVar, Generic, Optional
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook,
    TransformHook
)
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventTypes


T = TypeVar('T', EventHook, TransformHook)


class BaseEvent(Generic[T]):

    __slots__ = (
        'target',
        'pre_sources',
        'post_sources',
        'event_type',
        'pre',
        'as_hook'
    )

    def __init__(self, target: Hook, source: EventHook) -> None:

        target.is_event = True

        self.target = target
        self.pre_sources: Dict[str, T] = {}
        self.post_sources: Dict[str, T] = {}
        self.event_type = EventTypes.EVENT

        if source.pre:
            self.pre_sources[source.name] = source

        else:
            self.post_sources[source.name] = source

        if target:
            self.target_name = self.target.name
            self.target_shortname = self.target.shortname

        self.pre = source.pre
        self.as_hook = False

    def __getattribute__(self, name: str) -> Any:
        
        target = object.__getattribute__(self, 'target')

        if target and hasattr(target, name) and name != 'call':
            return getattr(target, name)
        
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: Any) -> None:

        try:

            target = object.__getattribute__(self, 'target')

            if target and hasattr(target, name) and name != 'call':
                return setattr(target, name, value)

        except AttributeError:
            pass

        return super().__setattr__(name, value)

    async def call(self, *args, **kwargs):
        raise NotImplementedError('Err. - This method is not implemented for the BaseEvent type.')

    async def execute_pre(self, result: Optional[Any]):
        raise NotImplementedError('Err. - This method is not implemented for the BaseEvent type.')

    async def execute_post(self, result: Optional[Any]):
        raise NotImplementedError('Err. - This method is not implemented for the BaseEvent type.')