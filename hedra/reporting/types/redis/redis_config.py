from typing import Optional
from hedra.reporting.types.common.types import ReporterTypes
from pydantic import BaseModel


class RedisConfig(BaseModel):
    host: str='localhost:6379'
    username: Optional[str]
    password: Optional[str]
    database: int=0
    events_channel: str='events'
    metrics_channel: str='metrics'
    channel_type: str='pipeline'
    secure: bool=False
    reporter_type: ReporterTypes=ReporterTypes.Redis