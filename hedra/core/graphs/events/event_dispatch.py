import asyncio
from collections import OrderedDict
from typing import List, Union, Dict
from .base_event import BaseEvent
from .event_types import EventType



class EventDispatcher:

    def __init__(self, timeout: Union[int, float]=60) -> None:
        self.events: OrderedDict[EventType, List[BaseEvent]]= OrderedDict()
        self.priority_map = {
            EventType.CONTEXT: 0,
            EventType.EVENT: 1,
            EventType.TRANSFORM: 2,
            EventType.CONDITION: 3
        }

        event_orderings = list(sorted(
            list(self.priority_map.items()),
            key=lambda event: event[1]
        ))

        for event_type_name, _ in event_orderings:
            self.events[event_type_name] = []
            

        self.timeout = timeout

    def __iter__(self):
        for event_type in self.events:
            for event in self.events[event_type]:
                yield event

    def __getitem__(self, event_type: EventType):
        return self.events[event_type]

    def set_events(self, events: List[BaseEvent]) -> None:
        for event in events:
            self.events[event.event_type].append(event)       

    def add_event(self, event: BaseEvent):
        self.events[event.event_type].append(event)

    async def dispatch_events(self):
        return {
            event_type: await asyncio.gather(*[
                asyncio.create_task(
                    asyncio.wait_for(
                        event.call(),
                        timeout=self.timeout
                    )
                ) for event in self.events[event_type]
            ]) for event_type in self.events
        }

    