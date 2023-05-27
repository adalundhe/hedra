from typing import Optional
from hedra.reporting.types.common.types import ReporterTypes
from pydantic import BaseModel


class MongoDBConfig(BaseModel):
    host: str='localhost:27017'
    username: Optional[str]
    password: Optional[str]
    database: str='hedra'
    events_collection: str='events'
    metrics_collection: str='metrics'
    experiments_collection: str='experiment'
    streams_collection: str='streams'
    system_metrics_collection='system_metrics'
    reporter_type: ReporterTypes=ReporterTypes.MongoDB