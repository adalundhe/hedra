
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import TelegrafClient
    from .telegraf_config import TelegrafConfig
    has_connector = True

except ImportError:
    from hedra.reporting.types.empty import Empty as StatsD
    TelegrafConfig = None
    TelegrafClient = None
    has_connector = False


class Telegraf(StatsD):

    def __init__(self, config: TelegrafConfig) -> None:
        super(Telegraf, self).__init__(config)

        self.connection = TelegrafClient(
            host=self.host,
            port=self.port
        )

    async def submit_events(self, events: List[BaseEvent]):

        for event in events:
            self.connection.send_telegraf(event.name, {'time': event.time})
            
            if event.success:
                self.connection.send_telegraf(event.name, {'success': 1})
            
            else:
                self.connection.send_telegraf(event.name, {'failed': 1})

    async def submit_metrics(self, metrics: List[Metric]):

        for metric in metrics:
            
            record = metric.stats

            for metric_field, metric_value in record.items():
                if metric_value and metric_field in self.types_map:
                    update_type = self.types_map.get(metric_field)

                    self.connection.send_telegraf(metric_field, {metric_field: metric_value})