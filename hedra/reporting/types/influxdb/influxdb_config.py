from hedra.reporting.types.common.types import ReporterTypes


class InfluxDBConfig:
    host: str=None
    token: str=None
    organization: str=None
    connect_timeout: int=10000
    events_bucket: str=None
    metrics_bucket: str=None
    reporter_type: ReporterTypes=ReporterTypes.InfluxDB