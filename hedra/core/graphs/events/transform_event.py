from collections import defaultdict
from hedra.core.graphs.hooks.registry.registry_types import TransformHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


class TransformEvent(BaseEvent[TransformHook]):

    def __init__(self, target: Hook, source: TransformHook) -> None:
        super(
            TransformEvent,
            self
        ).__init__(
            target,
            source
        )
        
        self.event_type = EventType.TRANSFORM

    def copy(self):
        transform_event = TransformEvent(
            self.target.copy(),
            self.source.copy()
        )

        transform_event.execution_path = list(self.execution_path)
        transform_event.previous_map = list(self.previous_map)
        transform_event.next_map = list(self.next_map)
        transform_event.next_args = defaultdict(dict)

        return transform_event