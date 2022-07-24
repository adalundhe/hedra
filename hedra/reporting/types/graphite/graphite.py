
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


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

    async def submit_metrics(self, metrics: List[Metric]):

        for metric in metrics:
            
            record = metric.stats

            for metric_field, metric_value in record.items():
                if metric_value and metric_field in self.types_map:
                    self.connection.send_graphite(metric_field, metric_value)