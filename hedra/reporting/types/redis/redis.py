import json
from typing import Any, List

try:

    import aioredis
    has_connector = True

except ImportError:
    has_connector = True


class Redis:

    def __init__(self, config: Any) -> None:
        self.host = config.host
        self.username = config.username
        self.password = config.password
        self.database = config.database or 0
        self.events_channel = config.events_channel
        self.metrics_channel = config.metrics_channel
        self.type = config.type or 'pipeline'
        self.connection = None

    async def connect(self):
        self.connection = await aioredis.from_url(
            self.host,
            username=self.username,
            password=self.password,
            db=self.database
        )

    async def submit_events(self, events: List[Any]):

        if self.type == 'channel':
            
            for event in events:
                await self.connection.publish(
                    self.events_channel,
                    json.dumps(event.record)
                )

        else:
            for event in events:
                await self.connection.sadd(
                    self.events_channel,
                    json.dumps(events.record)
                )

    async def submit_metrics(self, metrics: List[Any]):

        if self.type == 'channel':

            for metric in metrics:
                await self.connection.publish(
                    self.metrics_channel,
                    json.dumps(metric.record)
                )

        else:
            for metric in metrics:
                await self.connection.sadd(
                    self.metrics_channel,
                    json.dumps(metric.record)
                )

    async def close(self):
        await self.connection.close()