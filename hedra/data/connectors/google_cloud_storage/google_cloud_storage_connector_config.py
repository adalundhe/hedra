from pydantic import BaseModel
from hedra.data.connectors.common.connector_type import ConnectorType


class GoogleCloudStorageConnectorConfig(BaseModel):
    service_account_json_path: str
    bucket_namespace: str
    bucket_name: str
    connector_type: ConnectorType=ConnectorType.GCS