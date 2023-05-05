from types import SimpleNamespace
from typing import Dict
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes



class BigQueryConfig(BaseModel):
    service_account_json_path: str
    project_name: str
    dataset_name: str
    events_table: str = 'events'
    metrics_table: str = 'metrics'
    experiments_table: str= 'experiments'
    streams_table: str='streams'
    retry_timeout: int = 10
    reporter_type: ReporterTypes=ReporterTypes.BigQuery

    class Config:
        arbitrary_types_allowed = True
