from typing import Optional
from hedra.data.connectors.common.connector_type import ConnectorType
from pydantic import (
    BaseModel,
    StrictStr
)


class MongoDBConnectorConfig(BaseModel):
    host: StrictStr='localhost:27017'
    username: Optional[StrictStr]
    password: Optional[StrictStr]
    database: StrictStr
    collection: StrictStr
    connector_type: ConnectorType=ConnectorType.MongoDB