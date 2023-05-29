from typing import Any, Dict, Optional
from hedra.data.connectors.common.connector_type import ConnectorType
from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictInt,
    StrictBool
)


class KafkaConnectorConfig(BaseModel):
    host: StrictStr='localhost:9092'
    client_id: StrictStr='hedra'
    topic: StrictStr
    partition: StrictInt=0
    compression_type: Optional[StrictStr]
    timeout: StrictInt=1000
    idempotent: StrictBool=True
    options: Dict[StrictInt, Any]={}
    reporter_type: ConnectorType=ConnectorType.Kafka