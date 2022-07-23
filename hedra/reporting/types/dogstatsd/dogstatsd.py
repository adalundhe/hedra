
from typing import Any


try:
    from hedra.reporting.types.statsd import StatsD
    from aio_statsd import DogStatsdClient
    has_connector = True

except ImportError:
    has_connector = False


class DogStatsD(StatsD):

    def __init__(self, config: Any) -> None:
        super().__init__(config)

        self.connection = DogStatsdClient(
            host=self.host,
            port=self.port
        )

        self._update_map = {
            'count': self.connection.increment,
            'gauge': self.connection.gauge,
            'sets': lambda: NotImplementedError('DogStatsD does not support sets.'),
            'increment': self.connection.increment,
            'histogram': self.connection.histogram,
            'distribution': self.connection.distribution,
            'timer': self.connection.timer
        }