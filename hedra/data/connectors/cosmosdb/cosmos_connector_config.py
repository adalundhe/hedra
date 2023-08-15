from pydantic import BaseModel
from hedra.data.connectors.common.connector_type import ConnectorType


class CosmosDBConnectorConfig(BaseModel):
    account_uri: str
    account_key: str
    database: str
    container_name: str
    partition_key: str
    analytics_ttl: int=0
    connector_type: ConnectorType=ConnectorType.CosmosDB