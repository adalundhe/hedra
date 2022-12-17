import re
import uuid
from typing import List

from numpy import float32, float64, int16, int32, int64
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    from aio_statsd import StatsdClient
    from .statsd_config import StatsDConfig
    has_connector = True

except Exception:
    StatsdClient = None
    StatsDConfig = None
    has_connector = False


class StatsD:

    def __init__(self, config: StatsDConfig) -> None:
        self.host = config.host
        self.port = config.port

        self.connection = StatsdClient(
            host=self.host,
            port=self.port
        )

        self.types_map = {
            'total': 'count',
            'succeeded': 'count',
            'failed': 'count',
            'actions_per_second': 'gauge',
            'median': 'gauge',
            'mean': 'gauge',
            'variance': 'gauge',
            'stdev': 'gauge',
            'minimum': 'gauge',
            'maximum': 'gauge',
            'quantiles': 'gauge'
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

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.statsd_type = 'StatsD'

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to {self.statsd_type} at - {self.host}:{self.port}')
        await self.connection.connect()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to {self.statsd_type} at - {self.host}:{self.port}')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to {self.statsd_type}')

        for event in events:
            time_update_function = self._update_map.get('gauge')
            time_update_function(f'{event.name}_time', event.time)
            
            if event.success:
                success_update_function = self._update_map.get('count')
                success_update_function(f'{event.name}_success', 1)
            
            else:
                failed_update_function = self._update_map.get('count')
                failed_update_function(f'{event.name}_failed', 1)
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to {self.statsd_type}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to {self.statsd_type}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}')

                update_type = self.types_map.get(field)
                update_function = self._update_map.get(update_type)

                update_function(
                    f'{metrics_set.name}_common_{field}', value
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to {self.statsd_type}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to {self.statsd_type}')

        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():

                metric_record = {**group.stats, **group.custom}
                metric_types = {**self.types_map, **group.custom_schemas}

                for metric_field, metric_value in metric_record.items():
                    
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}')
                    
                    update_type = metric_types.get(metric_field)
                    update_function = self._update_map.get(update_type)
                    
                    update_function(
                        f'{metrics_set.name}_{group_name}_{metric_field}',
                        metric_value
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to {self.statsd_type}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to {self.statsd_type}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')
            
            for custom_group_name, group in metrics_set.custom_metrics.items():

                for field, value in group.items():
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{custom_group_name}:{field}')
                    
                    update_type = None
                    if isinstance(value, (int, int16, int32, int64)):
                        update_type = 'count'

                    elif isinstance(value, (float, float32, float64)):
                        update_type = 'gauge'

                    update_function = self._update_map.get(update_type)
                    update_function(
                        f'{metrics_set.name}_{custom_group_name}_{field}',
                        value
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to {self.statsd_type}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to {self.statsd_type}')

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                error_message = re.sub(
                    '[^0-9a-zA-Z]+', 
                    '_',
                    error.get(
                        'message'
                    ).lower()
                )

                update_function = self._update_map.get('count')
                update_function(f'{metrics_set.name}_errors_{error_message}', error.get('count'))


        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to {self.statsd_type}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection to {self.statsd_type} at - {self.host}:{self.port}')
        await self.connection.close()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection to {self.statsd_type} at - {self.host}:{self.port}')