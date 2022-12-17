
import uuid
from typing import List
from numpy import float32, float64, int16, int32, int64
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric.metrics_set import MetricsSet

try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import DogStatsdClient
    from .dogstatsd_config import DogStatsDConfig
    has_connector = True

except Exception:
    from hedra.reporting.types.empty import Empty as StatsD
    DogStatsdClient = None
    DogStatsDConfig = None
    has_connector = False


class DogStatsD(StatsD):

    def __init__(self, config: DogStatsDConfig) -> None:
        super(DogStatsD, self).__init__(config)
    
        self.host = config.host
        self.port = config.port

        self.connection = DogStatsdClient(
            host=self.host,
            port=self.port
        )

        self.types_map = {
            'total': 'increment',
            'succeeded': 'increment',
            'failed': 'increment',
            'actions_per_second': 'gauge',
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
            'count': lambda: NotImplementedError('DogStatsD does not support counts.'),
            'gauge': self.connection.gauge,
            'sets': lambda: NotImplementedError('DogStatsD does not support sets.'),
            'increment': self.connection.increment,
            'histogram': self.connection.histogram,
            'distribution': self.connection.distribution,
            'timer': self.connection.timer
        }

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.statsd_type = 'StatsD'

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to {self.statsd_type}')

        for event in events:
            time_update_function = self._update_map.get('gauge')
            time_update_function(f'{event.name}_time', event.time)
            
            if event.success:
                success_update_function = self._update_map.get('increment')
                success_update_function(f'{event.name}_success', 1)
            
            else:
                failed_update_function = self._update_map.get('increment')
                failed_update_function(f'{event.name}_failed', 1)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to {self.statsd_type}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to {self.statsd_type}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():

                for field, value in group.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metric - {metrics_set.name}:{custom_group_name}:{field}')
                    
                    update_type = None
                    if isinstance(value, (int, int16, int32, int64)):
                        update_type = 'increment'

                    elif isinstance(value, (float, float32, float64)):
                        update_type = 'gauge'

                    update_function = self._update_map.get(update_type)
                    update_function(
                        f'{metrics_set.name}_{custom_group_name}_{field}',
                        value
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to {self.statsd_type}')