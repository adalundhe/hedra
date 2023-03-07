from collections import defaultdict
from hedra.core.graphs.hooks.registry.registry_types import LoadHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


class LoadEvent(BaseEvent[LoadHook]):

    def __init__(self, target: Hook, source: LoadHook) -> None:
        super(
            LoadEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.LOAD

    def copy(self):
        load_event = LoadEvent(
            self.target.copy(),
            self.source.copy()
        )

        load_event.execution_path = list(self.execution_path)
        load_event.previous_map = list(self.previous_map)
        load_event.next_map = list(self.next_map)
        load_event.next_args = defaultdict(dict)

        return load_event