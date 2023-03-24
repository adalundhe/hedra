from collections import defaultdict
from hedra.core.hooks.types.event.hook import EventHook
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType


class Event(BaseEvent[EventHook]):

    def __init__(self, target: Hook, source: EventHook) -> None:
        super(
            Event,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.EVENT

    def copy(self):
        event = Event(
            self.target.copy(),
            self.source.copy()
        )

        event.execution_path = list(self.execution_path)
        event.previous_map = list(self.previous_map)
        event.next_map = list(self.next_map)
        event.next_args = defaultdict(dict)


        return event