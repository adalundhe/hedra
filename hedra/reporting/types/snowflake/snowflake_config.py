from types import SimpleNamespace
from typing import Dict, Optional
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes

try:
    import sqlalchemy

except Exception:
    sqlalchemy = SimpleNamespace(Column=None)


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
    custom_fields: Dict[str, sqlalchemy.Column]={}
    connect_timeout: int=30
    reporter_type: ReporterTypes=ReporterTypes.Snowflake

    class Config:
        arbitrary_types_allowed = True