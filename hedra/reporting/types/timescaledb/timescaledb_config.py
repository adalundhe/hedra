from types import SimpleNamespace
from typing import Dict
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes

try:
    import sqlalchemy
except Exception:
    sqlalchemy = SimpleNamespace(Column=None)


class TimescaleDBConfig(BaseModel):
    host: str='localhost'
    database: str
    username: str
    password: str
    events_table: str='events'
    metrics_table: str='metrics'
    custom_fields: Dict[str, sqlalchemy.Column]={}
    reporter_type: ReporterTypes=ReporterTypes.TimescaleDB

    class Config:
        arbitrary_types_allowed = True