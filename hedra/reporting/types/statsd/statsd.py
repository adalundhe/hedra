from typing import Any, List


try:
    from aio_statsd import StatsdClient
    has_connector = True

except ImportError:
    has_connector = False


class StatsD:

    def __init__(self, config: Any) -> None:
        self.host = config.host
        self.port = config.port
        self.custom_fields = config.custom_fields

        self.connection = StatsdClient(
            host=self.host,
            port=self.port
        )

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

        self._update_map = {
            'count': self.connection.counter,
            'gauge': self.connection.gauge,
            'increment': self.connection.gauge,
            'sets': self.connection.sets,
            'histogram': lambda: NotImplementedError('StatsD does not support histograms.'),
            'distribution': lambda: NotImplementedError('StatsD does not support distributions.'),
            'timer': self.connection.timer

        }

    async def connect(self):
        await self.connection.connect()

    async def submit_events(self, events: List[Any]):

        for event in events:
            
            record = event.record

            for event_field, event_value in record.items():
                if event_value and event_field in self.types_map:
                    update_type = self.types_map.get(event_field)
                    update_function = self._update_map.get(update_type)

                    update_function(event_field, event_value)

    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:
            
            record = metric.record

            for metric_field, metric_value in record.items():
                if metric_value and metric_field in self.types_map:
                    update_type = self.types_map.get(metric_field)
                    update_function = self._update_map.get(update_type)

                    update_function(metric_field, metric_value)

    async def close(self):
        await self.connection.close()