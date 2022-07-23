from typing import Any, List
try:
    from influxdb_client import Point
    from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
    has_connector = True

except ImportError:
    has_connector = False



class InfluxDB:

    def __init__(self, config: Any) -> None:
        self.host = config.host
        self.token = config.token
        self.organization = config.organization
        self.connect_timeout = config.connect_timeout or 10000
        self.events_bucket = config.events_bucket
        self.metrics_bucket = config.metrics_bucket

        self.client = None
        self.write_api = None

    async def connect(self):
        self.client = InfluxDBClientAsync(
            self.host,
            token=self.token,
            org=self.organization,
            timeout=self.connect_timeout
        )

        self.write_api = self.client.write_api()

    async def submit_events(self, events: List[Any]):

        points = []
        for event in events:
            point = Point(event.name)

            record = event.record
            del record['name']

            for tag in event.tags:
                point.tag(tag.name, tag.value)

            for field, value in record:
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self.events_bucket,
            record=points
        )

    async def submit_metrics(self, metrics: List[Any]):

        points = []
        for metric in metrics:
            point = Point(metric.name)

            record = metric.record
            del record['name']

            for tag in metric.tags:
                point.tag(tag.name, tag.value)

            for field, value in record:
                point.field(field, value)

            points.append(point)

        await self.write_api.write(
            bucket=self.metrics_bucket,
            record=points
        )

    async def close(self):
        await self.client.close()

            

                

