import uuid
import json
from typing import List, Dict, Any
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet

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
        self.custom_metrics_topics = {}
        self.shared_metrics_topic = 'stage_metrics'
        self.errors_topic = 'stage_errors'

        self.events_partition = config.events_partition
        self.metrics_partition = config.metrics_partition
        self.shared_metrics_partition = 'stage_metrics'
        self.errors_partition = 'stage_errors'

        self.compression_type = config.compression_type
        self.timeout = config.timeout
        self.enable_idempotence = config.idempotent or True
        self.options: Dict[str, Any] = config.options or {}
        self._producer = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Kafka at - {self.host}')

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - Compression Type: {self.compression_type}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - Connection Timeout: {self.timeout}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - Idempotent: {self.enable_idempotence}')

        for option_name, option in self.options.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - {option_name.capitalize()}: {option}')


        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.host,
            client_id=self.client_id,
            compression_type=self.compression_type,
            request_timeout_ms=self.timeout,
            enable_idempotence=self.enable_idempotence,
            **self.options
        )

        await self._producer.start()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Kafka at - {self.host}')

    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Topic - {self.events_topic} - Partition - {self.events_partition}')

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

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Topic - {self.events_topic} - Partition - {self.events_partition}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Topic - {self.shared_metrics_topic} - Partition - {self.shared_metrics_partition}')

        batch = self._producer.create_batch()
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            batch.append(
                value=json.dumps({
                    'name': metrics_set.name,
                    'stage': metrics_set.stage,
                    'group': 'common',
                    **metrics_set.common_stats
                }).encode('utf-8'),
                timestamp=None, 
                key=bytes(metrics_set.name, 'utf')
            )

        await self._producer.send_batch(
            batch,
            self.shared_metrics_topic,
            partition=self.shared_metrics_partition
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Topic - {self.shared_metrics_topic} - Partition - {self.shared_metrics_partition}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}')
        
        batch = self._producer.create_batch()
        for metrics_set in metrics:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for group_name, group in metrics_set.groups.items():
                batch.append(
                    value=json.dumps(
                        {
                            **group.record,
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': group_name
                        }
                    ).encode('utf-8'),
                    timestamp=None, 
                    key=bytes(f'{metrics_set.name}_{group_name}', 'utf')
                )

        await self._producer.send_batch(
            batch,
            self.metrics_topic,
            partition=self.metrics_partition
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}')
    
    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Customm Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for custom_group_name, group in metrics_set.custom_metrics.items():
                custom_topic_name = f'{custom_group_name}_metrics'

                if self.custom_metrics_topics.get(custom_topic_name) is None:
                    self.custom_metrics_topics[custom_topic_name] = self._producer.create_batch()
                
                self.custom_metrics_topics[custom_topic_name].append(
                    value=json.dumps({
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': custom_group_name,
                        **group
                    }).encode('utf-8'),
                    timestamp=None,
                    key=bytes(f'{metrics_set.name}_{custom_group_name}', 'utf')
                )

        for topic_name, batch in self.custom_metrics_topics.items():
            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Customm Metrics to Topic - {topic_name} - Partition - {topic_name}')
            await self._producer.send_batch(
                batch,
                topic_name,
                partition=topic_name
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Customm Metrics to Topic - {topic_name} - Partition - {topic_name}')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}')

        batch = self._producer.create_batch()
        for metrics_set in metrics_sets:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

            for error in metrics_set.errors:
                batch.append(
                    value=json.dumps(
                        {
                            'metric_name': metrics_set.name,
                            'metric_stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        }
                    ).encode('utf-8'),
                    timestamp=None, 
                    key=bytes(metrics_set.name, 'utf')
                )
        
        await self._producer.send_batch(
            batch,
            self.errors_topic,
            partition=self.errors_partition
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Topic - {self.metrics_topic} - Partition - {self.metrics_partition}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connection to Kafka at - {self.host}')

        await self._producer.stop()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connection to Kafka at - {self.host}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')