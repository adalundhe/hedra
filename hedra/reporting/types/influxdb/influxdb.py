from typing import Any, List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup

try:
    from influxdb_client import Point
    from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
    from .influxdb_config import InfluxDBConfig
    has_connector = True

except ImportError:
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

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        points = []
        for metrics_group in metrics:
            
            for timings_group_name, timings_group in metrics_group.groups.items():
                point = Point(metrics_group.name)

                for tag in metrics_group.tags:
                    point.tag(tag.name, tag.value)

                metric_record = {
                    **timings_group.stats, 
                    **timings_group.custom,
                    'timings_group': timings_group_name
                }

                for field, value in metric_record.items():
                    point.field(field, value)

        await self.write_api.write(
            bucket=self.metrics_bucket,
            record=points
        )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):
        points = []
        for metrics_group in metrics_groups:
            for error in metrics_group.errors:
                point = Point(metrics_group.name)
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

            

                

