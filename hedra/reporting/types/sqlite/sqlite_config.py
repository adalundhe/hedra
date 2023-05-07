import os
from pydantic import BaseModel

from hedra.reporting.types.common.types import ReporterTypes


class SQLiteConfig(BaseModel):
    path: str=f'{os.getcwd()}/results.db'
    events_table: str='events'
    metrics_table: str='metrics'
    experiments_table: str='experiments'
    streams_table: str='streams'
    reporter_type: ReporterTypes=ReporterTypes.SQLite

    class Config:
        arbitrary_types_allowed = True