import json
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric

try:

    from aiokafka import AIOKafkaProducer
    from .kafka_config import KafkaConfig
    has_connector = True

except ImportError:
    AIOKafkaProducer = None
    KafkaConfig = None
    has_connector = False


class Kafka:

    def __init__(self, config: KafkaConfig) -> None:
        self.host = config.host
        self.client_id = config.client_id
        self.events_topic = config.events_topic
        self.metrics_topic = config.metrics_topic
        self.events_partition = config.events_partition
        self.metrics_partition = config.metrics_partition
        self.compression_type = config.compression_type
        self.timeout = config.timeout
        self.enable_idempotence = config.idempotent or True
        self.options = config.options or {}
        self._producer = None

    async def connect(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.host,
            client_id=self.client_id,
            value_serializer=self._json_serializer,
            compression_type=self.compression_type,
            request_timeout_ms=self.timeout,
            enable_idempotence=self.enable_idempotence,
            **self.options
        )

    async def _json_serializer(self, value) -> bytes:
        return json.dumps(value).encode('utf-8')

    async def submit_events(self, events: List[BaseEvent]):
        for event in events:
            await self._producer.send_and_wait(
                self.events_topic,
                {
                    'event_value': event.record
                },
                key=event.name,
                partition=self.events_partition
            )

    async def submit_metrics(self, metrics: List[Metric]):
        for metric in metrics:
            await self._producer.send_and_wait(
                self.metrics_topic,
                {
                    'metric_value': metric.record
                },
                key=metric.name,
                partition=self.metrics_partition
            )

    async def close(self):
        await self._producer.stop()