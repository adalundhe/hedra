import uuid
from collections import defaultdict
from typing import Any, List
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet

try:
    from influxdb_client import Point
    from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
    from .influxdb_config import InfluxDBConfig
    has_connector = True

except Exception:
    Point = None
    InfluxDBClientAsync = None
    InfluxDBConfig = None
    has_connector = False


class InfluxDB:

    def __init__(self, config: InfluxDBConfig) -> None:
        self.host = config.host
        self.token = config.token
        self.protocol = 'https' if config.secure else 'http'
        self.organization = config.organization
        self.connect_timeout = config.connect_timeout
        self.events_bucket_name = config.events_bucket
        self.metrics_bucket_name = config.metrics_bucket
        self.shared_metrics_bucket_name = 'stage_metrics'
        self.errors_bucket_name = 'stage_errors'
        self.events_database = None
        self.metrics_database = None

        self.client = None
        self.write_api = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')

        self.client = InfluxDBClientAsync(
            f'{self.protocol}://{self.host}',
            token=self.token,
            org=self.organization,
            timeout=self.connect_timeout
        )

        self.write_api = self.client.write_api()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Bucket - {self.events_bucket_name}')

        points = []
        for event in events:
            point = Point(event.name)


            for tag in event.tags:
                point.tag(tag.name, tag.value)

            point.field(f'{event.name}_time', event.time)

            if event.success:
                point.field(f'{event.name}_success', 1)
            
            else:
                point.field(f'{event.name}_failed', 1)

            points.append(point)

        await self.write_api.write(
            bucket=self.events_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Bucket - {self.events_bucket_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Bucket - {self.shared_metrics_bucket_name}')

        points = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            point = Point(metrics_set.name)

            for tag in metrics_set.tags:
                point.tag(tag.name, tag.value)

            metric_record = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                **metrics_set.common_stats
            }

            for field, value in metric_record.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}')
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self.shared_metrics_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Bucket - {self.shared_metrics_bucket_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Bucket - {self.metrics_bucket_name}')

        points = []
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            for group_name, group in metrics_set.groups.items():
                point = Point(metrics_set.name)

                for tag in metrics_set.tags:
                    point.tag(tag.name, tag.value)

                metric_record = {
                    **group.stats, 
                    **group.custom,
                    'group': group_name
                }

                for field, value in metric_record.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:{group_name}:{field}')
                    point.field(field, value)

                points.append(point)

        await self.write_api.write(
            bucket=self.metrics_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Bucket - {self.metrics_bucket_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        points = defaultdict(list)
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():

                point = Point(f'{metrics_set.name}_{custom_group_name}')

                for tag in metrics_set.tags:
                    point.tag(tag.name, tag.value)
                
                metric_record = {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
                }

                for field, value in metric_record.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:{group_name}:{field}')
                    point.field(field, value)

                points[custom_group_name].append(point)

        for group_name, points in points.items():
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to Bucket - {group_name}_metrics')
            await self.write_api.write(
                bucket=f'{group_name}_metrics',
                record=points
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to Bucket - {group_name}_metrics')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Errors Metrics to Bucket - {self.errors_bucket_name}')

        points = []
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Errors Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                point = Point(f'{metrics_set.name}_errors')
                point.field(
                    error.get('message'),
                    error.get('count')
                )

            points.append(point)

        await self.write_api.write(
            bucket=self.errors_bucket_name,
            record=points
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Errors Metrics to Bucket - {self.errors_bucket_name}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')
        
        await self.client.close()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to InfluxDB at - {self.protocol}://{self.host} - for Organization - {self.organization}')
