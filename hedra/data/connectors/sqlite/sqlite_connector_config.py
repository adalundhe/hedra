import os
from pydantic import (
    BaseModel,
    StrictStr
)

from hedra.data.connectors.common.connector_type import ConnectorType


class SQLiteConnectorConfig(BaseModel):
    path: StrictStr=f'{os.getcwd()}/results.db'
    table_name:StrictStr
    reporter_type: ConnectorType=ConnectorType.SQLite

    class Config:
        arbitrary_types_allowed = True