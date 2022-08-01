from types import SimpleNamespace
from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes
from pydantic import BaseModel

try:
    import sqlalchemy
except Exception:
    sqlalchemy = SimpleNamespace(Column=None)


class PostgresConfig(BaseModel):
    host: str='localhost'
    database: str
    username: str
    password: str
    events_table: str='events'
    metrics_table: str='metrics'
    custom_fields: Dict[str, sqlalchemy.Column]={}
    reporter_type: ReporterTypes=ReporterTypes.Postgres

    class Config:
        arbitrary_types_allowed = True