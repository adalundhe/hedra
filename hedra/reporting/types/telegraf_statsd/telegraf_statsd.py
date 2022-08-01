
from typing import Any, List
from hedra.reporting.events.types.base_event import BaseEvent


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import TelegrafStatsdClient
    from .teleraf_statsd_config import TelegrafStatsDConfig
    has_connector = True

except Exception:
    from hedra.reporting.types.empty import Empty as StatsD
    TelegrafStatsDConfig=None
    TelegrafStatsdClient = None
    has_connector = False


class TelegrafStatsD(StatsD):

    def __init__(self, config: TelegrafStatsDConfig) -> None:
        super().__init__(config)
        self.connection = TelegrafStatsdClient(
            host=self.host,
            port=self.port
        )

        self.types_map = {
            'total': 'increment',
            'succeeded': 'increment',
            'failed': 'increment',
            'median': 'gauge',
            'mean': 'gauge',
            'variance': 'gauge',
            'stdev': 'gauge',
            'minimum': 'gauge',
            'maximum': 'gauge',
            'quantiles': 'gauge'
        }

        self._update_map = {
            'count': lambda: NotImplementedError('TelegrafStatsD does not support counts.'),
            'gauge': self.connection.gauge,
            'sets': lambda: NotImplementedError('TelegrafStatsD does not support sets.'),
            'increment': self.connection.increment,
            'histogram': self.connection.histogram,
            'distribution': self.connection.distribution,
            'timer': self.connection.timer
        }

    async def submit_events(self, events: List[BaseEvent]):

        for event in events:
            time_update_function = self._update_map.get('gauge')
            time_update_function(f'{event.name}_time', event.time)
            
            if event.success:
                success_update_function = self._update_map.get('increment')
                success_update_function(f'{event.name}_success', 1)
            
            else:
                failed_update_function = self._update_map.get('increment')
                failed_update_function(f'{event.name}_failed', 1)