from hedra.reporting.types.common.types import ReporterTypes
from pydantic import BaseModel


class PostgresConfig(BaseModel):
    host: str='localhost'
    database: str
    username: str
    password: str
    events_table: str='events'
    metrics_table: str='metrics'
    experiments_table: str='experiments'
    streams_table: str='streams'
    reporter_type: ReporterTypes=ReporterTypes.Postgres

    class Config:
        arbitrary_types_allowed = True