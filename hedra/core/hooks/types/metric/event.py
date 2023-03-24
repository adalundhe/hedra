from collections import defaultdict
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.hooks.types.base.event_types import EventType
from .hook import MetricHook


class MetricEvent(BaseEvent[MetricHook]):

    def __init__(self, target: Hook, source: MetricHook) -> None:
        super(
            MetricEvent,
            self
        ).__init__(
            target,
            source
        )

        self.event_type = EventType.METRIC

    def copy(self):
        metric_event = MetricEvent(
            self.target.copy(),
            self.source.copy()
        )

        metric_event.execution_path = list(self.execution_path)
        metric_event.previous_map = list(self.previous_map)
        metric_event.next_map = list(self.next_map)
        metric_event.next_args = defaultdict(dict)

        return metric_event