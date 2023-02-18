from collections import defaultdict
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

    def copy(self):
        check_event = CheckEvent(
            self.target.copy(),
            self.source.copy()
        )

        check_event.execution_path = list(self.execution_path)
        check_event.previous_map = list(self.previous_map)
        check_event.next_map = list(self.next_map)
        check_event.next_args = defaultdict(dict)

        return check_event