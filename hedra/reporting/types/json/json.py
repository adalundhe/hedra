import json
from typing import Any, List
from .json_config import JSONConfig
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric
has_connector = True

class JSON:

    def __init__(self, config: JSONConfig) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath

    async def connect(self):
        pass

    async def submit_events(self, events: List[BaseEvent]):
        event_records = [
            event.record for event in events
        ]

        with open(self.events_filepath, 'w') as events_file:
            json.dumps(events_file, event_records, indent=4)

    async def submit_metrics(self, metrics: List[Metric]):
        metrics_records = [
            metric.record for metric in metrics
        ]

        with open(self.metrics_filepath, 'w') as metrics_file:
            json.dumps(metrics_records, metrics_file, indent=4)

    async def close(self):
        pass