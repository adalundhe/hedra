import asyncio
from typing import Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import CheckHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


class CheckEvent(BaseEvent[CheckHook]):

    def __init__(self, target: Hook, source: CheckHook) -> None:
        super(
            CheckEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.CHECK
