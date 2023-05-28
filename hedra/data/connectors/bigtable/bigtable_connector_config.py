from hedra.connectors.common.connector_type import ConnectorType
from pydantic import BaseModel, StrictStr


class BigTableConnectorConfig(BaseModel):
    service_account_json_path: StrictStr
    instance_id: StrictStr
    table_name: StrictStr
    reporter_type: ConnectorType=ConnectorType.BigTable

