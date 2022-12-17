import json
import uuid
from typing import List
from hedra.logging import HedraLogger
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
        self.shared_metrics_channel = f'{self.metrics_channel}_metrics'
        self.errors_channel = f'{self.metrics_channel}_errors'

        self.channel_type = config.channel_type
        self.connection = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Redis instance at - {self.base}://{self.host} - Database: {self.database}')
        self.connection = await aioredis.from_url(
            f'{self.base}://{self.host}',
            username=self.username,
            password=self.password,
            db=self.database
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Redis instance at - {self.base}://{self.host} - Database: {self.database}')

    async def submit_events(self, events: List[BaseEvent]):

        if self.channel_type == 'channel':
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Channel - {self.errors_channel}')
            
            for event in events:
                await self.connection.publish(
                    self.events_channel,
                    json.dumps(event.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Channel - {self.errors_channel}')

        else:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Redis Set - {self.errors_channel}')
            for event in events:
                await self.connection.sadd(
                    self.events_channel,
                    json.dumps(events.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Redis Set - {self.errors_channel}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            if self.channel_type == 'channel':
                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Channel - {self.shared_metrics_channel}')
                await self.connection.publish(
                    self.shared_metrics_channel,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Channel - {self.shared_metrics_channel}')

            else:
                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Redis Set - {self.shared_metrics_channel}')
                await self.connection.sadd(
                    self.shared_metrics_channel,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        **metrics_set.common_stats
                    })
                )

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Redis Set - {self.shared_metrics_channel}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                if self.channel_type == 'channel':
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Channel - {self.metrics_channel} - Group: {group_name}')
                    await self.connection.publish(
                        self.metrics_channel,
                        json.dumps({
                            **group.record,
                            'group': group_name
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Channel - {self.metrics_channel} - Group: {group_name}')

                else:
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Redis Set - {self.metrics_channel} - Group: {group_name}')
                    await self.connection.sadd(
                        self.metrics_channel,
                        json.dumps({
                            **group.record,
                            'group': group_name
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Redis Set - {self.metrics_channel} - Group: {group_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_metrics_channel_name = f'{custom_group_name}_metrics'

                if self.channel_type == 'channel':
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Channel - {custom_metrics_channel_name} - Group: {custom_group_name}')
                    await self.connection.publish(
                        custom_metrics_channel_name,
                        json.dumps({
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Channel - {custom_metrics_channel_name} - Group: {custom_group_name}')

                else:
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Redis Set - {custom_metrics_channel_name} - Group: {custom_group_name}')
                    await self.connection.sadd(
                        custom_metrics_channel_name,
                        json.dumps({
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Redis Set - {custom_metrics_channel_name} - Group: {custom_group_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:

                if self.channel_type == 'channel':
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Channel - {self.errors_channel}')
                    await self.connection.publish(
                        self.errors_channel,
                        json.dumps({
                            'metrics_name': metrics_set.name,
                            'metrics_stage': metrics_set.stage,
                            'errors_message': error.get('message'),
                            'errors_count': error.get('count')
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Channel - {self.errors_channel}')

                else:
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Reids Set - {self.errors_channel}')
                    await self.connection.sadd(
                        self.errors_channel,
                        json.dumps({
                            'metrics_name': metrics_set.name,
                            'metrics_stage': metrics_set.stage,
                            'errors_message': error.get('message'),
                            'errors_count': error.get('count')
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Reids Set - {self.errors_channel}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to Redis at - {self.base}://{self.host}')

        await self.connection.close()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to Redis at - {self.base}://{self.host}')