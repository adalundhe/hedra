import uuid
import os
from async_tools.functions.awaitable import awaitable

from hedra.reporting.connectors.types.kafka_connector.models.types import message_set
from .serializer import Serializer
from aiokafka import AIOKafkaProducer



class Producer:

    def __init__(self, config):
        self.producer_host = config.get(
            'kafka_server_host',
            os.getenv('KAFKA_SERVER_HOST', '127.0.0.1')
        )
        self.producer_port = config.get(
            'kafka_server_port',
            os.getenv('KAFKA_SERVER_PORT', 9092)
        )
        self.producer_address = '{producer_host}:{producer_port}'.format(
            producer_host=self.producer_host,
            producer_port=self.producer_port
        )
        self.producer_client_id = config.get(
            'kafka_producer_client_id',
            str(uuid.uuid4())
        )
        self.type = 'producer'
        self._producer = None
        self.options = config.get('options', {})
        self._serializer_config = config.get('kafka_serializer', {})
        self._serializer = None
        self.current_messages = []

        self._serializer = Serializer(self._serializer_config)
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.producer_address,
            client_id=self.producer_client_id,
            value_serializer=self._serializer.selected_serializer,
            compression_type=self.options.get('kafka_compression_type'),
            request_timeout_ms=self.options.get('kafka_producer_timeout', 1000),
            enable_idempotence=self.options.get('kafka_enable_idempotence', True),
            **self.options.get('kafka_producer_options', {})
        )

    async def connect(self) -> None:
        await self._producer.start()

    async def execute(self, messages) -> None:
        for message in messages:
            response = await self._producer.send_and_wait(
                message.topic,
                message.value,
                key=message.key,
                partition=message.partition
            )


            self.current_messages.append(response)

    async def commit(self) -> None:
        self.current_messages = []

    async def clear(self) -> None:
        await self._producer.flush()
        self.current_messages = []

    async def close(self) -> None:
        await self._producer.stop()

    


            
