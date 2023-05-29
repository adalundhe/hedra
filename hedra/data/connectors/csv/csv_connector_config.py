from pydantic import BaseModel, StrictStr
from hedra.data.connectors.common.connector_type import ConnectorType


class CSVConnectorConfig(BaseModel):
    filepath: StrictStr
    file_mode: StrictStr='r'
    reporter_type: ConnectorType=ConnectorType.CSV