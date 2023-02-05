import asyncio
from typing import Any, Tuple
from hedra.core.graphs.hooks.registry.registry_types import TaskHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


class TaskEvent(BaseEvent[TaskHook]):

    def __init__(self, target: Hook, source: TaskHook) -> None:
        super(
            TaskEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.TASK
