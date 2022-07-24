from hedra.reporting.types.common.types import ReporterTypes


class RedisConfig:
    host: str=None
    username: str=None
    password: str=None
    database: int=0
    events_channel: str=None
    metrics_channel: str=None
    channel_type: str='pipeline'
    reporter_type: ReporterTypes=ReporterTypes.Redis