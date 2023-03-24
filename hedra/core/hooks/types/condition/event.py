from collections import defaultdict
from hedra.core.hooks.types.condition.hook import ConditionHook
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType


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

        condtion_event.execution_path = list(self.execution_path)
        condtion_event.previous_map = list(self.previous_map)
        condtion_event.next_map = list(self.next_map)
        condtion_event.next_args = defaultdict(dict)

        return condtion_event