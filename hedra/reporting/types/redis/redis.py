import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


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
        self.stage_metrics_channel = 'stage_metrics'
        self.errors_channel = 'stage_errors'
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

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            if self.channel_type == 'channel':
                await self.connection.publish(
                    self.stage_metrics_channel,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

            else:
                await self.connection.sadd(
                    self.stage_metrics_channel,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        **metrics_set.common_stats
                    })
                )

    async def submit_metrics(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:
            for group_name, group in metrics_set.groups.items():
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

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_metrics_channel_name = f'{custom_group_name}_metrics'

                if self.channel_type == 'channel':
                    await self.connection.publish(
                        custom_metrics_channel_name,
                        json.dumps({
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

                else:
                    await self.connection.sadd(
                        custom_metrics_channel_name,
                        json.dumps({
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        
        for metrics_set in metrics_sets:
            for error in metrics_set.errors:

                if self.channel_type == 'channel':
                    await self.connection.publish(
                        self.errors_channel,
                        json.dumps({
                            'metrics_name': metrics_set.name,
                            'metrics_stage': metrics_set.stage,
                            'errors_message': error.get('message'),
                            'errors_count': error.get('count')
                        })
                    )

                else:
                    await self.connection.sadd(
                        self.errors_channel,
                        json.dumps({
                            'metrics_name': metrics_set.name,
                            'metrics_stage': metrics_set.stage,
                            'errors_message': error.get('message'),
                            'errors_count': error.get('count')
                        })
                    )

    async def close(self):
        await self.connection.close()