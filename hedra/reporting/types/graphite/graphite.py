
import re
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup, timings_group


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import GraphiteClient
    from .graphite_config import GraphiteConfig
    has_connector = True

except ImportError:
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

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:

            for timings_group_name, timings_group in metrics_group.groups.items():
            
                for metric_field, metric_value in timings_group.stats.items():
                    self.connection.send_graphite(
                        f'{metrics_group.name}_{timings_group_name}_{metric_field}', metric_value
                    )

                for metric_field, metric_value in timings_group.custom.items():
                    self.connection.send_graphite(
                        f'{metrics_group.name}_{timings_group_name}_{metric_field}', metric_value
                    )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:

            for error in metrics_group.errors:
                error_message = re.sub(
                    '[^0-9a-zA-Z]+', 
                    '_',
                    error.get(
                        'message'
                    ).lower()
                )

                self.connection.send_graphite(
                    f'{metrics_group.name}_{error_message}',
                    error.get('count')
                )
