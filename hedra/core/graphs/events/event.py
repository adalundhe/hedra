from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


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

        event.execution_path = self.execution_path
        event.previous_map = self.previous_map
        event.next_map = self.next_map
        event.next_args = self.next_args

        return event