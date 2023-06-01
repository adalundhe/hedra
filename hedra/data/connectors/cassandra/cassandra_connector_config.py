from ssl import SSLContext
from typing import List, Optional
from pydantic import BaseModel, StrictStr, StrictInt
from hedra.data.connectors.common.connector_type import ConnectorType


class CassandraConnectorConfig(BaseModel):
    hosts: List[StrictStr] = ['127.0.0.1']
    port: StrictInt=9042
    username: Optional[StrictStr]=None
    password: Optional[StrictStr]=None
    keyspace: StrictStr='hedra'
    table_name: StrictStr
    replication_strategy: StrictStr='SimpleStrategy'
    replication: StrictInt=3
    ssl: Optional[SSLContext]=None
    connector_type: ConnectorType=ConnectorType.Cassandra

    class Config:
        arbitrary_types_allowed=True