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

    def copy(self):
        condtion_event = ConditionEvent(
            self.target.copy(),
            self.source.copy()
        )

        condtion_event.execution_path = self.execution_path
        condtion_event.previous_map = self.previous_map
        condtion_event.next_map = self.next_map
        condtion_event.next_args = self.next_args

        return condtion_event