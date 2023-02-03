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
