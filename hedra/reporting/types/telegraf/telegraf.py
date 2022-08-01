
import re
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import TelegrafClient
    from .telegraf_config import TelegrafConfig
    has_connector = True

except Exception:
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

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            self.connection.send_telegraf(
                f'{metrics_set.name}_common',
                {
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'common',
                    **metrics_set.common_stats
                }
            )

    async def submit_metrics(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:

            for group_name, group in metrics_set.groups.items():
                self.connection.send_telegraf(
                    f'{metrics_set.name}_{group_name}', 
                    {
                        **group.record,
                        'group': group_name
                    }
                )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            for custom_group_name, group in metrics_set.custom_metrics.items():
                self.connection.send_telegraf(
                    f'{metrics_set.name}_{custom_group_name}',
                    {
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': custom_group_name,
                        **group
                    }
                )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:

            for error in metrics_set.errors:
                self.connection.send_telegraf(
                    f'{metrics_set.name}_errors', {
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    })