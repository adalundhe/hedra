import asyncio
from typing import Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import ConditionHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


class ConditionEvent(BaseEvent[ConditionHook]):

    def __init__(self, target: Hook, source: ConditionHook) -> None:
        super(
            ConditionEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.CONDITION
