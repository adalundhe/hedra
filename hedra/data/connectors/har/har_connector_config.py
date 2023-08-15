from hedra.data.connectors.common.connector_type import ConnectorType
from pydantic import (
    BaseModel,
    StrictStr
)


class HARConnectorConfig(BaseModel):
    filepath: StrictStr
    connector_type: ConnectorType=ConnectorType.CSV