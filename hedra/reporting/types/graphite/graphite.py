
import re
import uuid
from typing import List
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import GraphiteClient
    from .graphite_config import GraphiteConfig
    has_connector = True

except Exception:
    from hedra.reporting.types.empty import Empty as StatsD
    GraphiteClient = None
    GraphiteConfig = None
    has_connector = False


class Graphite(StatsD):

    def __init__(self, config: GraphiteConfig) -> None:
        super().__init__(config)

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.connection = GraphiteClient(
            host=self.host,
            port=self.port
        )

        self.statsd_type = 'Graphite'

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to {self.statsd_type}')

        for event in events:
            self.connection.send_graphite(f'{event.name}_time', event.time)
            
            if event.success:
                self.connection.send_graphite(f'{event.name}_success', 1)
            
            else:
                self.connection.send_graphite(f'{event.name}_failed', 1)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to {self.statsd_type}')


    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to {self.statsd_type}')
        
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for field, value in metrics_set.common_stats.items():
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metric - {metrics_set.name}:common:{field}')

                self.connection.send_graphite(
                    f'{metrics_set.name}_common_{field}', value
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to {self.statsd_type}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to {self.statsd_type}')

        for metrics_set in metrics:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
            
                for metric_field, metric_value in group.stats.items():

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}')

                    self.connection.send_graphite(
                        f'{metrics_set.name}_{group_name}_{metric_field}', metric_value
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitted Metric - {metrics_set.name}:{group_name}:{metric_field}')

                for metric_field, metric_value in group.custom.items():

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{group_name}:{metric_field}')

                    self.connection.send_graphite(
                        f'{metrics_set.name}_{group_name}_{metric_field}', metric_value
                    )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to {self.statsd_type}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to {self.statsd_type}')

        for metrics_set in metrics_sets:

            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():

                for field, value in group.items():

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metric - {metrics_set.name}:{custom_group_name}:{field}')

                    self.connection.send_graphite(
                        f'{metrics_set.name}_{custom_group_name}_{field}', value
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

                self.connection.send_graphite(
                    f'{metrics_set.name}_error_{error_message}',
                    error.get('count')
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to {self.statsd_type}')