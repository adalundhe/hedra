from collections import defaultdict
from hedra.core.graphs.hooks.registry.registry_types import ContextHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from .event_types import EventType
from .base_event import BaseEvent


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