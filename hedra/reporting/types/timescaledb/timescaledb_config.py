from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class TimescaleDBConfig(BaseModel):
    host: str='localhost'
    database: str
    username: str
    password: str
    events_table: str='events'
    metrics_table: str='metrics'
    experiments_table: str='experiments'
    streams_table: str='streams'
    reporter_type: ReporterTypes=ReporterTypes.TimescaleDB

    class Config:
        arbitrary_types_allowed = True