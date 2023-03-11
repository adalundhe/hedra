from collections import defaultdict
from hedra.core.hooks.types.context.hook import ContextHook
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType


class ContextEvent(BaseEvent[ContextHook]):

    def __init__(self, target: Hook, source: ContextHook) -> None:

        super(
            ContextEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.CONTEXT

    def copy(self):
        context_event = ContextEvent(
            self.target.copy(),
            self.source.copy()
        )

        context_event.execution_path = list(self.execution_path)
        context_event.previous_map = list(self.previous_map)
        context_event.next_map = list(self.next_map)
        context_event.next_args = defaultdict(dict)

        return context_event