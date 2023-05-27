from typing import Any, Dict, Optional
from hedra.reporting.types.common.types import ReporterTypes
from pydantic import BaseModel


class KafkaConfig(BaseModel):
    host: str='localhost:9092'
    client_id: str='hedra'
    events_topic: str='events'
    metrics_topic: str='metrics'
    experiments_topic: str='experiments'
    streams_topic: str='streams'
    system_metrics_topic='system_metrics'
    events_partition: int=0
    metrics_partition: int=0
    experiments_partition: int=0
    streams_partition: int=0
    system_metrics_partition: int=0
    compression_type: Optional[str]
    timeout: int=1000
    idempotent: bool=True
    options: Dict[str, Any]={}
    reporter_type: ReporterTypes=ReporterTypes.Kafka