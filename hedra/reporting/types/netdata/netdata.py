from typing import Any


try:
    from hedra.reporting.types.statsd.statsd import StatsD
    has_connector = True

except ImportError:
    has_connector = False


class Netdata(StatsD):

    def __init__(self, config: Any) -> None:
        super().__init__(config)