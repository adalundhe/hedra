import asyncio
from typing import Any, Tuple
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
