from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventTypes
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

        self.event_type = EventTypes.EVENT

