from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class GoogleCloudStorageConfig(BaseModel):
    service_account_json_path: str
    bucket_namespace: str
    events_bucket: str='events'
    metrics_bucket: str='metrics'
    experiments_bucket: str='experiments'
    streams_bucket: str='stages'
    system_metrics_bucket: str='system_metrics'
    reporter_type: ReporterTypes=ReporterTypes.GCS