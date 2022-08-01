from collections import defaultdict
from typing import Any, List
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
        self.events_bucket = config.events_bucket
        self.metrics_bucket = config.metrics_bucket
        self.errors_bucket = f'{self.metrics_bucket}_errors'
        self.events_database = None
        self.metrics_database = None

        self.client = None
        self.write_api = None

    async def connect(self):
        self.client = InfluxDBClientAsync(
            f'{self.protocol}://{self.host}',
            token=self.token,
            org=self.organization,
            timeout=self.connect_timeout
        )

        self.write_api = self.client.write_api()

    async def submit_events(self, events: List[BaseEvent]):

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
            bucket=self.events_bucket,
            record=points
        )

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        points = []
        for metrics_set in metrics_sets:
            point = Point(f'{metrics_set.name}_group_metrics')

            for tag in metrics_set.tags:
                point.tag(tag.name, tag.value)

            metric_record = {
                'name': metrics_set.name,
                'stage': metrics_set.stage,
                **metrics_set.common_stats
            }

            for field, value in metric_record.items():
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self.metrics_bucket,
            record=points
        )

    async def submit_metrics(self, metrics: List[MetricsSet]):

        points = []
        for metrics_set in metrics:
            
            for group_name, group in metrics_set.groups.items():
                point = Point(f'{metrics_set.name}_metrics')

                for tag in metrics_set.tags:
                    point.tag(tag.name, tag.value)

                metric_record = {
                    **group.stats, 
                    **group.custom,
                    'group': group_name
                }

                for field, value in metric_record.items():
                    point.field(field, value)

                points.append(point)

        await self.write_api.write(
            bucket=self.metrics_bucket,
            record=points
        )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        points = defaultdict(list)
        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():

                point = Point(f'{metrics_set.name}_{custom_group_name}_metrics')

                for tag in metrics_set.tags:
                    point.tag(tag.name, tag.value)
                
                metric_record = {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': custom_group_name,
                    **group
                }

                for field, value in metric_record.items():
                    point.field(field, value)

                points[custom_group_name].append(point)

        for group_name, points in points.items():
            await self.write_api.write(
                bucket=f'{self.metrics_bucket}_{group_name}',
                record=points
            )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):
        points = []
        for metrics_set in metrics_sets:
            for error in metrics_set.errors:
                point = Point(f'{metrics_set.name}_errors')
                point.field(
                    error.get('message'),
                    error.get('count')
                )

            points.append(point)

        await self.write_api.write(
            bucket=self.errors_bucket,
            record=points
        )

    async def close(self):
        await self.client.close()

            

                

