import json
from typing import Generic, TypeVar
from hedra.reporting.events.types.base_event import BaseEvent


T = TypeVar('T')

class Event(BaseEvent, Generic[T]):

    __slots__ = (
        'fields',
        'type'
    )

    def __init__(self, result: T) -> None:
        super().__init__(result)

        self.timings = result.as_timings()
        self.time = result.times['complete'] - result.times['start']

    def to_dict(self):
        return {
            'name': self.name,
            'stage': self.stage,
            'shortname': self.shortname,
            'checks': [check.__name__ for check in self.checks],
            'error': str(self.error),
            'time': self.time,
            'type': self.type,
            'source': self.source,
        }

    def serialize(self):
        return json.dumps(self.to_dict())