from collections import defaultdict
from hedra.core.hooks.types.transform.hook import TransformHook
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType


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