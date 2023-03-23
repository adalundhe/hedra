from __future__ import annotations
from collections import defaultdict
from typing import Any, Dict, TypeVar, Generic, List
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.hooks.types.base.hook import Hook
from .event_types import EventType


T = TypeVar('T')


class BaseEvent(Generic[T]):

    __slots__ = (
        'target',
        'event_type',
        'source',
        'as_hook',
        'event_name',
        'event_order',
        'target_is_event',
        'context',
        'events',
        'execution_path',
        'next_args',
        'previous_map',
        'next_map'
    )

    def __init__(self, target: Hook, source: Hook) -> None:
        
        target.is_event = True
        self.target = target
        self.target_is_event = False

        if isinstance(target, BaseEvent):
            self.target_is_event = True

        self.event_type = EventType.EVENT
        self.event_name = source.name
        self.event_order = source.order

        self.source: T = source

        if target:
            self.target_name = self.target.name
            self.target_shortname = self.target.shortname

        self.as_hook = False
        self.context: SimpleContext = SimpleContext()
        self.events: Dict[str, BaseEvent] = {}
        self.execution_path = []
        self.previous_map = []
        self.next_map = []
        self.next_args: Dict[str, Dict[str,Any]] = defaultdict(dict)

    def __getattribute__(self, name: str) -> Any:
        
        source = None
        event_attrs = [
            'call', 
            'event_type',
            'target', 
            'source',
            'as_hook',
            'event_name',
            'event_order',
            'target_is_event',
            'context',
            'events',
            'execution_path',
            'next_args',
            'previous_map',
            'next_map',
            'copy'
        ]
      
        source = object.__getattribute__(self, 'source')
        
        if source and hasattr(source, name) and name not in event_attrs:
            return getattr(source, name)
        
        return object.__getattribute__(self, name)

    def __setattr__(self, name: str, value: Any) -> None:

        try:

            source = object.__getattribute__(self, 'source')

            event_attrs = [
                'call', 
                'event_type',
                'target', 
                'source',
                'as_hook',
                'event_name',
                'event_order',
                'target_is_event',
                'context',
                'events',
                'execution_path',
                'next_args',
                'previous_map',
                'next_map',
                'copy'
            ]

            if source and hasattr(source, name) and name not in event_attrs:
                return setattr(source, name, value)

        except AttributeError:
            pass

        return super().__setattr__(name, value)

    async def call(self, **kwargs) -> Dict[str, Any]: 
   
        if len(self.next_args[self.event_name]) == 0:
            self.next_args[self.event_name] = kwargs

        if isinstance(self.source, BaseEvent):
            self.source.context = self.context

        results = await self.source.call(**self.next_args[self.event_name])
        

        self.context.update(results)
        self.source.stage_instance.context.update(results)

        if self.source.context:
            self.source.context.update(results)

        next_events = [
            self.events.get(event_name) for event_name in  self.next_map if self.events.get(event_name) is not None
        ]

        for event in next_events:
            event.context = self.context

            self.next_args[event.event_name].update(results)

        return {
            self.event_name: results
        }

    async def execute_pre(self, *hook_args: List[Any]):
        results = None
        for source_name in self.previous_map:
            source: BaseEvent = self.events.get(source_name)
            source.context = self.context
            results = await source.call(*hook_args)

        return results

    async def execute_post(self, results):
        results = None
        for source_name in self.previous_map:
            source: BaseEvent = self.events.get(source_name)
            source.context = self.context
            results = await source.call(results)

        return results

    def copy(self) -> BaseEvent[Hook]:
        base_event = BaseEvent(
            self.target.copy(),
            self.source.copy()
        )

        base_event.execution_path = self.execution_path
        base_event.previous_map = self.previous_map
        base_event.next_map = self.next_map
        base_event.next_args = self.next_args

        return base_event
