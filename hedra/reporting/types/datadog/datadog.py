import asyncio
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:
    import datadog
    from .datadog_config import DatadogConfig
    has_connector = True

except ImportError:
    datadog = None
    DatadogConfig = None
    has_connector = False

from datetime import datetime
from typing import Any, List


class Datadog:

    def __init__(self, config: DatadogConfig) -> None:
        self.datadog_api_key = config.api_key
        self.datadog_app_key = config.app_key
        self.event_alert_type = config.event_alert_type or 'info'
        self.device_name = config.device_name or 'hedra'
        self.priority = config.priority
        self.custom_fields = config.custom_fields or {}

        self.types_map = {
            'total': 'count',
            'succeeded': 'count',
            'failed': 'count',
            'median': 'gauge',
            'mean': 'gauge',
            'variance': 'gauge',
            'stdev': 'gauge',
            'minimum': 'gauge',
            'maximum': 'gauge',
            'quantiles': 'gauge',
            **self.custom_fields
        }

        self._loop = asyncio.get_event_loop()

    async def connect(self):
        config = {
            'api_key': self.datadog_api_key,
            'app_key': self.datadog_app_key

        }

        datadog.initialize(**config)

    async def submit_events(self, events: List[BaseEvent]):

        for event in events:

            tags = {
                f'{tag.name}:{tag.value}' for tag in event.tags
            }

            await self._loop.run_in_executor(
                None,
                datadog.api.Event.create,
                title=event.name,
                text=event.data_as_string(),
                alert_type=self.event_alert_type,
                aggregation_key=event.type,
                device_name=self.device_name,
                date_happened=datetime.now().strftime('%Y-%m-%dT%H:%M:%S.Z'),
                priority=self.priority,
                tags=tags,
                host=event.hostname
            )

    async def submit_metics(self, metrics: List[Metric]):

        for metric in metrics:

            tags = [
                f'{tag.name}:{tag.value}' for tag in metric.tags
            ]

            record = metric.record

            for field in self.types_map:
                value = record.get(field)

                await self._loop.run_in_executor(
                    None,
                    datadog.api.Metric.send,
                    metrics=[{
                        'metric': f'{metric.name}_{field}',
                        'points': [value],
                        'host': metric.source,
                        'tags': tags,
                        'type': self.types_map.get(field)
                    }]
                )

    async def close(self):
        pass
