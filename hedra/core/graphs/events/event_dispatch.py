import asyncio
from collections import OrderedDict
from typing import List, Union, Dict, Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import (
    EventHook,
    TransformHook,
    ContextHook,
    ConditionHook
)
from .base_event import BaseEvent
from .event_types import EventType



class EventDispatcher:

    def __init__(self, timeout: Union[int, float]=None) -> None:
        self.events: OrderedDict[EventType, List[BaseEvent]]= OrderedDict()
        self.priority_map = {
            EventType.CONTEXT: 0,
            EventType.LOAD: 1,
            EventType.EVENT: 2,
            EventType.TRANSFORM: 2,
            EventType.CONDITION: 2,
            EventType.SAVE: 3,
        }

        event_orderings = list(sorted(
            list(self.priority_map.items()),
            key=lambda event: event[1]
        ))

        for event_type_name, _ in event_orderings:
            self.events[event_type_name] = []
            
        self.events_by_name: Dict[str, BaseEvent] = {}
        self.timeout = timeout
        self.initial_events: List[BaseEvent] = []
        self.source_name = None

    def __iter__(self):
        for event_type in self.events:
            for event in self.events[event_type]:
                yield event

    def __getitem__(self, event_type: EventType):
        return self.events[event_type]

    def set_events(self, events: List[BaseEvent]) -> None:
        for event in events:
            self.events_by_name[event.event_name] = event
            self.events[event.event_type].append(event)       

    def add_event(self, event: BaseEvent):
        self.events_by_name[event.event_name] = event
        self.events[event.event_type].append(event)

    async def dispatch_events(self):
        await asyncio.gather(*[
            asyncio.create_task(
                self._execute_batch(initial_event)
            ) for initial_event in self.initial_events
        ])
            

    async def _execute_batch(self, initial_event: BaseEvent):

        for layer in initial_event.execution_path:
                layer_events = [
                    self.events_by_name.get(event_name) for event_name in layer
                ]

                results: List[Dict[str, Any]] = await asyncio.gather(*[
                    asyncio.create_task(
                        asyncio.wait_for(
                            event.call(**event.next_args),
                            timeout=self.timeout
                        ) if self.timeout else event.call()
                    ) for event in layer_events
                ])

                result_events: List[Tuple[BaseEvent, Any]] = []
                for result in results:
                    for event_name, result in result.items():

                        event = self.events_by_name.get(event_name)
                        result_events.append((event, result))

                for event, result in result_events:
                    next_events = [
                        event.events.get(event_name) for event_name in  event.next_map if event.events.get(event_name) is not None
                    ]

                    event.context.update(event.next_args[event.event_name])
                    event.source.stage_instance.context.update(event.next_args[event.event_name])
                

                    for next_event in next_events:
                        if isinstance(event, (BaseEvent, ConditionHook)):

                            next_event.context.update(event.context)

                        next_event.next_args[next_event.event_name].update(result)
                        event.context.update(next_event.next_args[next_event.event_name])
                        event.source.stage_instance.context.update(next_event.next_args[next_event.event_name])
                
