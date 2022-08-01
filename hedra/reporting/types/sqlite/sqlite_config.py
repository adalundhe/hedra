import os
from types import SimpleNamespace
from typing import Dict
from pydantic import BaseModel
from pydantic.dataclasses import dataclass

from hedra.reporting.types.common.types import ReporterTypes

try:
    import sqlalchemy
except Exception:
    sqlalchemy = SimpleNamespace(Column=None)


class SQLiteConfig(BaseModel):
    path: str=f'{os.getcwd()}/results.db'
    events_table: str='events'
    metrics_table: str='metrics'
    custom_fields: Dict[str, sqlalchemy.Column]={}
    reporter_type: ReporterTypes=ReporterTypes.SQLite

    class Config:
        arbitrary_types_allowed = True