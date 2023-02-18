from collections import defaultdict
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

    def copy(self):
        task_event = TaskEvent(
            self.target.copy(),
            self.source.copy()
        )

        task_event.execution_path = list(self.execution_path)
        task_event.previous_map = list(self.previous_map)
        task_event.next_map = list(self.next_map)
        task_event.next_args = defaultdict(dict)

        return task_event