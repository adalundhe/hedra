
import re
from typing import List
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

        self.connection = GraphiteClient(
            host=self.host,
            port=self.port
        )

    async def submit_events(self, events: List[BaseEvent]):

         for event in events:
            self.connection.send_graphite(f'{event.name}_time', event.time)
            
            if event.success:
                self.connection.send_graphite(f'{event.name}_success', 1)
            
            else:
                self.connection.send_graphite(f'{event.name}_failed', 1)

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        for metrics_set in metrics_sets:
            for field, value in metrics_set.common_stats.items():
                self.connection.send_graphite(
                    f'{metrics_set.name}_{field}', value
                )

    async def submit_metrics(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:

            for group_name, group in metrics_set.groups.items():
            
                for metric_field, metric_value in group.stats.items():
                    self.connection.send_graphite(
                        f'{metrics_set.name}_{group_name}_{metric_field}', metric_value
                    )

                for metric_field, metric_value in group.custom.items():
                    self.connection.send_graphite(
                        f'{metrics_set.name}_{group_name}_{metric_field}', metric_value
                    )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():
                for field, value in group.items():
                    self.connection.send_graphite(
                        f'{metrics_set.name}_{custom_group_name}_{field}', value
                    )


    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:

            for error in metrics_set.errors:
                error_message = re.sub(
                    '[^0-9a-zA-Z]+', 
                    '_',
                    error.get(
                        'message'
                    ).lower()
                )

                self.connection.send_graphite(
                    f'{metrics_set.name}_{error_message}',
                    error.get('count')
                )
