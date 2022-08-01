from datetime import datetime
import json
from typing import List
import uuid
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup

try:

    from aiokafka import AIOKafkaProducer
    from .kafka_config import KafkaConfig
    has_connector = True

except Exception:
    AIOKafkaProducer = None
    KafkaConfig = None
    has_connector = False


class Kafka:

    def __init__(self, config: KafkaConfig) -> None:
        self.host = config.host
        self.client_id = config.client_id
        self.events_topic = config.events_topic
        self.metrics_topic = config.metrics_topic
        self.group_metrics_topic = f'{self.metrics_topic}_group_metrics'
        self.errors_topic = f'{self.metrics_topic}_errors'
        self.events_partition = config.events_partition
        self.metrics_partition = config.metrics_partition
        self.group_metrics_partition = f'{self.metrics_partition}_group_metrics'
        self.errors_partition = f'{self.metrics_partition}_errors'
        self.compression_type = config.compression_type
        self.timeout = config.timeout
        self.enable_idempotence = config.idempotent or True
        self.options = config.options or {}
        self._producer = None

    async def connect(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.host,
            client_id=self.client_id,
            compression_type=self.compression_type,
            request_timeout_ms=self.timeout,
            enable_idempotence=self.enable_idempotence,
            **self.options
        )

        await self._producer.start()

    async def submit_events(self, events: List[BaseEvent]):

        batch = self._producer.create_batch()
        for event in events:

            batch.append(
                value=json.dumps(
                    event.record
                ).encode('utf-8'),
                timestamp=None, 
                key=bytes(event.name, 'utf')
            )

        await self._producer.send_batch(
            batch,
            self.events_topic,
            partition=self.events_partition
        )

    async def submit_common(self, metrics_groups: List[MetricsGroup]):
        batch = self._producer.create_batch()
        for metrics_group in metrics_groups:
            batch.append(
                value=json.dumps({
                    'name': metrics_group.name,
                    'stage': metrics_group.stage,
                    **metrics_group.common_stats
                }).encode('utf-8'),
                timestamp=None, 
                key=bytes(metrics_group.name, 'utf')
            )

        await self._producer.send_batch(
            batch,
            self.group_metrics_topic,
            partition=self.group_metrics_partition
        )

    async def submit_metrics(self, metrics: List[MetricsGroup]):
        
        batch = self._producer.create_batch()
        for metrics_group in metrics:
            for group_name, group in metrics_group.groups.items():
                batch.append(
                    value=json.dumps(
                        {
                            **group.record,
                            'name': metrics_group.name,
                            'stage': metrics_group.stage,
                            'group': group_name
                        }
                    ).encode('utf-8'),
                    timestamp=None, 
                    key=bytes(metrics_group.name, 'utf')
                )

        await self._producer.send_batch(
            batch,
            self.metrics_topic,
            partition=self.metrics_partition
        )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):
        batch = self._producer.create_batch()
        for metric_group in metrics_groups:
            for error in metric_group.errors:
                batch.append(
                    value=json.dumps(
                        {
                            'metric_name': metric_group.name,
                            'metric_stage': metric_group.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        }
                    ).encode('utf-8'),
                    timestamp=None, 
                    key=bytes(metric_group.name, 'utf')
                )
        
        await self._producer.send_batch(
            batch,
            self.errors_topic,
            partition=self.errors_partition
        )

    async def close(self):
        await self._producer.stop()