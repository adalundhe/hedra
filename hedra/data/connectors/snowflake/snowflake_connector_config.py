from typing import Optional
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt
)
from hedra.data.connectors.common.connector_type import ConnectorType

class SnowflakeConnectorConfig(BaseModel):
    username: StrictStr
    password: StrictStr
    organization_id: StrictStr
    account_id: StrictStr
    private_key: Optional[StrictStr]
    warehouse: StrictStr
    database: StrictStr
    database_schema: StrictStr='PUBLIC'
    table_name: StrictStr
    connect_timeout: StrictInt=30
    connector_type: ConnectorType=ConnectorType.Snowflake

    class Config:
        arbitrary_types_allowed = True