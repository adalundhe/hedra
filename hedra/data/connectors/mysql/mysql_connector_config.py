from hedra.data.connectors.common.connector_type import ConnectorType
from pydantic import (
    BaseModel,
    StrictStr
)



class MySQLConnectorConfig(BaseModel):
    host: StrictStr='127.0.0.1'
    database: StrictStr
    username: StrictStr
    password: StrictStr
    table_name: StrictStr
    connector_type: ConnectorType=ConnectorType.MySQL

    class Config:
        arbitrary_types_allowed = True