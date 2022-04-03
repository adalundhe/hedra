import uuid
import os
from aiokafka import AIOKafkaConsumer
from .deserializer import Deserializer
from .results_set import ResultsSet
from async_tools.functions import awaitable


class Consumer:

    def __init__(self, config):
        self.config = config
        self.consumer_host = config.get(
            'kafka_server_host',
            os.getenv('KAFKA_SERVER_HOST', '127.0.0.1')
        )
        self.consumer_port = config.get(
            'kafka_server_port',
            os.getenv('KAFKA_SERVER_PORT', 9092)
        )
        self.consumer_address = '{consumer_host}:{consumer_port}'.format(
            consumer_host=self.consumer_host,
            consumer_port=self.consumer_port
        )
        self.consumer_client_id = config.get(
            'kafka_consumer_client_id',
            str(uuid.uuid4())
        )
        self.options = config.get('options', {})
        self.type = 'consumer'
        self.topics = config.get('message_topics', [])
        
        self._consumer = None
        self._results = ResultsSet()
        self._serializer_config = config.get('kafka_serializer', {})
        self._poll_interval = config.get('kafka_consumer_poll_interval', 0)
        self._deserializer = Deserializer(self._serializer_config)

        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self.consumer_address,
            client_id=self.consumer_client_id,
            group_id=self.options.get('kafka_consumer_group_id'),
            auto_offset_reset=self.options.get('auto_offset_reset', 'earliest'),
            enable_auto_commit=self.options.get('enable_auto_commit', True),
            value_deserializer=self._deserializer.selected_deserializer,
            request_timeout_ms=self.options.get('kafka_consumer_timeout', 1000),
            **self.options.get('kafka_consumer_options', {})
        )

        self.running = False
        self._poll_task = None


    async def connect(self) -> None:
        await self._consumer.start()

        topics = [
            topic.get('name') for topic in self.topics
        ]

        await awaitable(self._consumer.subscribe, topics)

    async def execute(self, transaction) -> None:
        messages = await self._consumer.getmany(
            max_records=transaction.options.get('max_records', 1),
            timeout_ms=transaction.options.get(
                'timeout',
                self.options.get('kafka_consumer_timeout', 1000)
            )
        )

        await self._results.add_message(messages)

    async def commit(self) -> list:
        results = await self._results.to_results()
        await self._results.clear()
        return results

    async def clear(self) -> None:
        await self._results.clear()

    async def close(self) -> None:
        self.running = False
        await self._consumer.stop()
        self._consumer.unsubscribe()

