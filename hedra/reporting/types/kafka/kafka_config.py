from typing import Any, Dict

from hedra.reporting.types.common.types import ReporterTypes


class KafkaConfig:
    host: str=None
    client_id: str=None
    events_topic: str=None
    metrics_topic: str=None
    events_partition: str=None
    metrics_partition: str=None
    compression_type: str=None
    timeout: int=1000
    idempotent: bool=True
    options: Dict[str, Any]={}
    reporter_type: ReporterTypes=ReporterTypes.Kafka