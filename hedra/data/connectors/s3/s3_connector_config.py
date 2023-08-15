from pydantic import (
    BaseModel,
    StrictStr
)
from hedra.data.connectors.common.connector_type import ConnectorType


class S3ConnectorConfig(BaseModel):
    aws_access_key_id: StrictStr
    aws_secret_access_key: StrictStr
    region_name: StrictStr
    bucket_name: StrictStr
    connector_type: ConnectorType=ConnectorType.S3