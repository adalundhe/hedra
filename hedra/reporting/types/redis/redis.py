import json
import uuid
from typing import List, Dict
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiments_collection import ExperimentMetricsCollectionSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.metric import MetricsSet
from .redis_config import RedisConfig


try:

    import aioredis
    
    has_connector = True

except Exception:
    aioredis = None
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
        self.streams_channel = config.streams_channel

        self.experiments_channel = config.experiments_channel
        self.variants_channel = f'{config.experiments_channel}_variants'
        self.mutations_channel = f'{config.experiments_channel}_mutations'

        self.shared_metrics_channel = f'{self.metrics_channel}_metrics'
        self.errors_channel = f'{self.metrics_channel}_errors'
        self.custom_metrics_channel = f'{self.metrics_channel}_custom'

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

    async def submit_streams(self, stream_metrics: Dict[str, StageStreamsSet]):

        for stage_name, stream in stream_metrics.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Streams - {stage_name}:{stream.stream_set_id}')

            for group_name, group in stream.grouped.items():
                if self.channel_type == 'channel':
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Channel - {self.streams_channel} - Group: {group_name}')
                    await self.connection.publish(
                        self.streams_channel,
                        json.dumps({
                            **group,
                            'group': group_name,
                            'stage': stage_name,
                            'name': f'{stage_name}_streams'
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Channel - {self.streams_channel} - Group: {group_name}')

                else:
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Streams to Redis Set - {self.streams_channel} - Group: {group_name}')
                    await self.connection.sadd(
                        self.streams_channel,
                        json.dumps({
                            **group,
                            'group': group_name,
                            'stage': stage_name,
                            'name': f'{stage_name}_streams'
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Streams to Redis Set - {self.streams_channel} - Group: {group_name}')

    async def submit_experiments(self, experiment_metrics: ExperimentMetricsCollectionSet):

        if self.channel_type == 'channel':
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Channel - {self.experiments_channel}')
            
            for experiment in experiment_metrics.experiment_summaries:
                await self.connection.publish(
                    self.experiments_channel,
                    json.dumps(experiment.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Channel - {self.experiments_channel}')

        else:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Experiments to Redis Set - {self.experiments_channel}')
            for experiment in experiment_metrics.experiment_summaries:
                await self.connection.sadd(
                    self.experiments_channel,
                    json.dumps(experiment.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Experiments to Redis Set - {self.experiments_channel}')

    async def submit_variants(self, experiment_metrics: ExperimentMetricsCollectionSet):

        if self.channel_type == 'channel':
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Channel - {self.variants_channel}')
            
            for variant in experiment_metrics.variant_summaries:
                await self.connection.publish(
                    self.variants_channel,
                    json.dumps(variant.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Channel - {self.variants_channel}')

        else:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Variants to Redis Set - {self.variants_channel}')
            for variant in experiment_metrics.variant_summaries:
                await self.connection.sadd(
                    self.variants_channel,
                    json.dumps(variant.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Variants to Redis Set - {self.variants_channel}')

    async def submit_mutations(self, experiment_metrics: ExperimentMetricsCollectionSet):

        if self.channel_type == 'channel':
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Channel - {self.mutations_channel}')
            
            for mutation in experiment_metrics.mutation_summaries:
                await self.connection.publish(
                    self.mutations_channel,
                    json.dumps(mutation.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Channel - {self.mutations_channel}')

        else:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Mutations to Redis Set - {self.mutations_channel}')
            for mutation in experiment_metrics.mutation_summaries:
                await self.connection.sadd(
                    self.mutations_channel,
                    json.dumps(mutation.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Mutations to Redis Set - {self.mutations_channel}')

    async def submit_events(self, events: List[BaseProcessedResult]):

        if self.channel_type == 'channel':
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Channel - {self.events_channel}')
            
            for event in events:
                await self.connection.publish(
                    self.events_channel,
                    json.dumps(event.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Channel - {self.events_channel}')

        else:
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Redis Set - {self.events_channel}')
            for event in events:
                await self.connection.sadd(
                    self.events_channel,
                    json.dumps(events.record)
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Redis Set - {self.events_channel}')

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

            if self.channel_type == 'channel':
                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Channel - {self.custom_metrics_channel} - Group: Custom')
                await self.connection.publish(
                    self.custom_metrics_channel,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'custom',
                        **{
                            custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                        }
                    })
                )

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Channel - {self.custom_metrics_channel} - Group: Custom')

            else:
                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Redis Set - {self.custom_metrics_channel} - Group: Custom')
                await self.connection.sadd(
                    self.custom_metrics_channel,
                    json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'custom',
                        **{
                            custom_metric_name: custom_metric.metric_value for custom_metric_name, custom_metric in metrics_set.custom_metrics.items()
                        }
                    })
                )

                await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Redis Set - {self.custom_metrics_channel} - Group: Custom')

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