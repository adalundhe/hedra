from types import SimpleNamespace
from typing import Dict

from hedra.reporting.types.common.types import ReporterTypes

try:
    import sqlalchemy
except ImportError:
    sqlalchemy = SimpleNamespace(Column=None)


class TimescaleDBConfig:
    host: str=None
    database: str=None
    username: str=None
    password: str=None
    events_table: str=None
    metrics_table: str=None
    custom_fields: Dict[str, sqlalchemy.Column]={}
    reporter_type: ReporterTypes=ReporterTypes.TimescaleDB