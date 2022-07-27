from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    from aio_statsd import StatsdClient
    from .statsd_config import StatsDConfig
    has_connector = True

except ImportError:
    StatsdClient = None
    StatsDConfig = None
    has_connector = False


class StatsD:

    def __init__(self, config: StatsDConfig) -> None:
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
            'increment': self.connection.increment,
            'sets': self.connection.sets,
            'histogram': lambda: NotImplementedError('StatsD does not support histograms.'),
            'distribution': lambda: NotImplementedError('StatsD does not support distributions.'),
            'timer': self.connection.timer

        }

    async def connect(self):
        await self.connection.connect()

    async def submit_events(self, events: List[BaseEvent]):

        for event in events:
            time_update_function = self._update_map.get('gauge')
            time_update_function(f'{event.name}_time', event.time)
            
            if event.success:
                success_update_function = self._update_map.get('count')
                success_update_function(f'{event.name}_success', 1)
            
            else:
                failed_update_function = self._update_map.get('count')
                failed_update_function(f'{event.name}_failed', 1)

    async def submit_metrics(self, metrics: List[Metric]):

        for metric in metrics:

            for metric_field, metric_value in metric.stats.items():
                if metric_value and metric_field in self.types_map:
                    update_type = self.types_map.get(metric_field)
                    update_function = self._update_map.get(update_type)

                    update_function(f'{metric.name}_{metric_field}', metric_value)

    async def close(self):
        await self.connection.close()