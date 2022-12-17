try:
    from hedra.reporting.types.statsd.statsd import StatsD
    from .netdata_config import NetdataConfig
    has_connector = True

except Exception:
    from hedra.reporting.types.empty import Empty as StatsD
    NetdataConfig = None
    has_connector = False


class Netdata(StatsD):

    def __init__(self, config: NetdataConfig) -> None:
        super(Netdata, self).__init__(config)
        self.statsd_type = 'Netdata'