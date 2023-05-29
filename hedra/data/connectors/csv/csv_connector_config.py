from hedra.data.connectors.common.connector_type import ConnectorType
from pydantic import BaseModel, StrictStr
from typing import List, Optional


class CSVConnectorConfig(BaseModel):
    filepath: StrictStr
    file_mode: StrictStr='r'
    reporter_type: ConnectorType=ConnectorType.CSV
    headers: Optional[List[StrictStr]]