import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


try:

    import aioredis
    from .redis_config import RedisConfig
    has_connector = True

except Exception:
    aioredis = None
    RedisConfig = None
    has_connector = True


class Redis:

    def __init__(self, config: RedisConfig) -> None:
        self.host = config.host
        self.base = 'rediss' if config.secure else 'redis'
        self.username = config.username
        self.password = config.password
        self.database = config.database
        self.events_channel = config.events_channel
        self.metrics_channel = config.metrics_channel
        self.group_metrics_channel = f'{self.group_metrics_channel}_group_metrics'
        self.errors_channel = f'{self.metrics_channel}_errors'
        self.channel_type = config.channel_type
        self.connection = None

    async def connect(self):
        self.connection = await aioredis.from_url(
            f'{self.base}://{self.host}',
            username=self.username,
            password=self.password,
            db=self.database
        )

    async def submit_events(self, events: List[BaseEvent]):

        if self.channel_type == 'channel':
            
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

    async def submit_common(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:
            if self.channel_type == 'channel':
                await self.connection.publish(
                    self.group_metrics_channel,
                    json.dumps({
                        'name': metrics_group.name,
                        'stage': metrics_group.stage,
                        **metrics_group.common_stats
                    })
                )

            else:
                await self.connection.sadd(
                    self.group_metrics_channel,
                    json.dumps({
                        'name': metrics_group.name,
                        'stage': metrics_group.stage,
                        **metrics_group.common_stats
                    })
                )

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:
            for group_name, group in metrics_group.groups.items():
                if self.channel_type == 'channel':
                    await self.connection.publish(
                        self.metrics_channel,
                        json.dumps({
                            **group.record,
                            'group': group_name
                        })
                    )

                else:
                    await self.connection.sadd(
                        self.metrics_channel,
                        json.dumps({
                            **group.record,
                            'group': group_name
                        })
                    )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):
        
        for metrics_group in metrics_groups:
            for error in metrics_group.errors:

                if self.channel_type == 'channel':
                    await self.connection.publish(
                        self.errors_channel,
                        json.dumps({
                            'metrics_name': metrics_group.name,
                            'metrics_stage': metrics_group.stage,
                            'errors_message': error.get('message'),
                            'errors_count': error.get('count')
                        })
                    )

                else:
                    await self.connection.sadd(
                        self.errors_channel,
                        json.dumps({
                            'metrics_name': metrics_group.name,
                            'metrics_stage': metrics_group.stage,
                            'errors_message': error.get('message'),
                            'errors_count': error.get('count')
                        })
                    )

    async def close(self):
        await self.connection.close()