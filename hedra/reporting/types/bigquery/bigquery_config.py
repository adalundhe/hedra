from types import SimpleNamespace
from typing import Dict
from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


try:

    from google.cloud import bigquery
    has_connector = True

except Exception:
    bigquery = SimpleNamespace(SchemaField=None)
    has_connector = False


class BigQueryConfig(BaseModel):
    service_account_json_path: str
    project_name: str
    dataset_name: str
    events_table: str = 'events'
    metrics_table: str = 'metrics'
    retry_timeout: int = 10
    custom_fields: Dict[str, bigquery.SchemaField] = {}
    reporter_type: ReporterTypes=ReporterTypes.BigQuery

    class Config:
        arbitrary_types_allowed = True
