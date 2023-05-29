from pydantic import BaseModel
from hedra.reporting.types.common.types import ReporterTypes


class GoogleCloudStorageConnectorConfig(BaseModel):
    service_account_json_path: str
    bucket_namespace: str
    bucket_name: str
    reporter_type: ReporterTypes=ReporterTypes.GCS