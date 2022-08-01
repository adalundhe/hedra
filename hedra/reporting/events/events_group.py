import asyncio
from collections import defaultdict
from typing import Any
from .types.base_event import BaseEvent
from .results import results_types


class EventsGroup:

    def __init__(self) -> None:
        self.events = []
        self.timings = defaultdict(list)
        self.tags = {}
        self.succeeded = 0
        self.failed = 0
        self.errors = defaultdict(lambda: 0)

    async def add(self, result: Any, stage_name: str):

        event = results_types.get(result.type)(result)
        event.stage = stage_name

        self.events.append(event)
        self.tags.update(event.tags_to_dict())

        await event.check_result()

        if event.error is None:
            self.succeeded += 1
        
        else:
            self.errors[event.error] += 1
            self.failed += 1
        
        if event.time > 0:
            self.timings['total'].append(event.time)

        if event.time_waiting > 0:
            self.timings['waiting'].append(event.time_waiting)

        if event.time_connecting > 0:
            self.timings['connecting'].append(event.time_connecting)

        if event.time_writing > 0:
            self.timings['writing'].append(event.time_writing)

        if event.time_reading > 0:
            self.timings['reading'].append(event.time_reading)
