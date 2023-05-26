from typing import Optional
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes

class SnowflakeConfig(BaseModel):
    username: str
    password: str
    organization_id: str
    account_id: str
    private_key: Optional[str]
    warehouse: str
    database: str
    database_schema: str='PUBLIC'
    events_table: str='events'
    metrics_table: str='metrics'
    experiments_table: str='experiments'
    streams_table: str='streams'
    system_metrics_table: str='system_metrics'
    connect_timeout: int=30
    reporter_type: ReporterTypes=ReporterTypes.Snowflake

    class Config:
        arbitrary_types_allowed = True