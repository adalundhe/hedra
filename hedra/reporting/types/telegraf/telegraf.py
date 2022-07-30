
import re
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import TelegrafClient
    from .telegraf_config import TelegrafConfig
    has_connector = True

except ImportError:
    from hedra.reporting.types.empty import Empty as StatsD

    has_connector = False


class Telegraf(StatsD):

    def __init__(self, config: TelegrafConfig) -> None:
        self.host = config.host
        self.port = config.port

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

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:

            for timings_group_name, timings_group in metrics_group.groups.items():
                self.connection.send_telegraf(
                    f'{metrics_group.name}_{timings_group_name}', 
                    timings_group.record
                )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:

            for error in metrics_group.errors:
                self.connection.send_telegraf(
                    f'{metrics_group.name}_errors', {
                        'metric_name': metrics_group.name,
                        'metric_stage': metrics_group.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    })