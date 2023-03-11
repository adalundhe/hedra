from collections import defaultdict
from hedra.core.graphs.hooks.types.save.hook import SaveHook
from hedra.core.graphs.hooks.types.base.hook import Hook
from ..hooks.types.base.event_types import EventType
from ..hooks.types.base.event import BaseEvent


class SaveEvent(BaseEvent[SaveHook]):

    def __init__(self, target: Hook, source: SaveHook) -> None:
        super(
            SaveEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.SAVE

    def copy(self):
        save_event = SaveEvent(
            self.target.copy(),
            self.source.copy()
        )

        save_event.execution_path = list(self.execution_path)
        save_event.previous_map = list(self.previous_map)
        save_event.next_map = list(self.next_map)
        save_event.next_args = defaultdict(dict)

        return save_event