
from typing import Any, List


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import TelegrafClient
    has_connector = True

except ImportError:
    has_connector = False


class Telegraf(StatsD):

    def __init__(self, config: Any) -> None:
        super().__init__(config)

        self.connection = TelegrafClient(
            host=self.host,
            port=self.port
        )

    async def submit_events(self, events: List[Any]):

        for event in events:
            
            record = event.stats

            for event_field, event_value in record.items():
                if event_value and event_field in self.types_map:

                    self.connection.send_telegraf(event_field, {event_field: event_value})

    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:
            
            record = metric.stats

            for metric_field, metric_value in record.items():
                if metric_value and metric_field in self.types_map:
                    update_type = self.types_map.get(metric_field)

                    self.connection.send_telegraf(metric_field, {metric_field: metric_value})