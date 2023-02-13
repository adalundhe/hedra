import asyncio
from typing import Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import SaveHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


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

        save_event.execution_path = self.execution_path
        save_event.previous_map = self.previous_map
        save_event.next_map = self.next_map
        save_event.next_args = self.next_args

        return save_event