import os
from pydantic import (
    BaseModel,
    StrictStr,
    StrictBool
)
from hedra.data.connectors.common.connector_type import ConnectorType


class XMLConnectorConfig(BaseModel):
    filepath: StrictStr
    file_mode: StrictStr='r'
    reporter_type: ConnectorType=ConnectorType.XML
