
from cProfile import label
from typing import Any


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import TelegrafStatsdClient
    has_connector = True

except ImportError:
    has_connector = False


class TelegrafStatsD(StatsD):

    def __init__(self, config: Any) -> None:
        super().__init__(config)

        self.connection = TelegrafStatsdClient(
            host=self.host,
            port=self.port
        )

        self._update_map = {
            'count': self.connection.increment,
            'gauge': self.connection.gauge,
            'sets': lambda: NotImplementedError('TelegrafStatsD does not support sets.'),
            'increment': self.connection.increment,
            'histogram': self.connection.histogram,
            'distribution': self.connection.distribution,
            'timer': self.connection.timer
        }