
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent

try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import DogStatsdClient
    from .dogstatsd_config import DogStatsDConfig
    has_connector = True

except Exception:
    from hedra.reporting.types.empty import Empty as StatsD
    DogStatsdClient = None
    DogStatsDConfig = None
    has_connector = False


class DogStatsD(StatsD):

    def __init__(self, config: DogStatsDConfig) -> None:
        super(DogStatsD, self).__init__(config)
    
        self.host = config.host
        self.port = config.port

        self.connection = DogStatsdClient(
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
            'quantiles': 'gauge',
            **self.custom_fields
        }

        self._update_map = {
            'count': lambda: NotImplementedError('DogStatsD does not support counts.'),
            'gauge': self.connection.gauge,
            'sets': lambda: NotImplementedError('DogStatsD does not support sets.'),
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