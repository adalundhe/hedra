from types import SimpleNamespace
from typing import Dict
from hedra.reporting.types.common.types import ReporterTypes


try:

    from google.cloud import bigquery
    has_connector = True

except ImportError:
    bigquery = SimpleNamespace(SchemaField=None)
    has_connector = False


class BigQueryConfig:
    token: str = None
    events_table: str = None
    metrics_table: str = None
    custom_fields: Dict[str, bigquery.SchemaField] = {}
    reporter_type: ReporterTypes=ReporterTypes.BigQuery
