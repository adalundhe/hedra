from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class S3Config(BaseModel):
    aws_access_key_id: str
    aws_secret_access_key: str
    region_name: str
    buckets_namespace: str
    events_bucket: str='events'
    metrics_bucket: str='metrics'
    experiments_bucket: str='experiments'
    streams_bucket: str='streams'
    reporter_type: ReporterTypes=ReporterTypes.S3