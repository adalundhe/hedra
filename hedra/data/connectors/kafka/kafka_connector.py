import uuid
import asyncio
import json
from typing import (
    List, 
    Dict, 
    Any,
    Union
)
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.parsers.parser import Parser
from .kafka_connector_config import KafkaConnectorConfig

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
has_connector = True

# try:

#     from aiokafka import AIOKafkaConsumer
#     has_connector = True

# except Exception:
#     AIOKafkaConsumer = None
#     has_connector = False


class Kafka:

    def __init__(
        self, 
        config: KafkaConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        
        self.host = config.host
        self.client_id = config.client_id
        self.stage = stage
        self.parser_config = parser_config

        self.topic = config.topic
        self.partition = config.partition

        self.compression_type = config.compression_type
        self.timeout = config.timeout
        self.enable_idempotence = config.idempotent or True
        self.options: Dict[str, Any] = config.options or {}
        self._consumer = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.loop: Union[asyncio.AbstractEventLoop, None] = None

        self.logger = HedraLogger()
        self.logger.initialize()
        self.parser = Parser()

    async def connect(self):

        self.loop = asyncio.get_event_loop()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Kafka at - {self.host}')

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - Compression Type: {self.compression_type}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - Connection Timeout: {self.timeout}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - Idempotent: {self.enable_idempotence}')

        for option_name, option in self.options.items():
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Using Kafka Options - {option_name.capitalize()}: {option}')


        self._consumer = AIOKafkaConsumer(
            self.topic,
            loop=self.loop,
            bootstrap_servers=self.host,
            client_id=self.client_id,
            compression_type=self.compression_type,
            request_timeout_ms=self.timeout,
            enable_idempotence=self.enable_idempotence,
            **self.options
        )

        await self._consumer.start()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Kafka at - {self.host}')

    async def load_actions(
        self,
        options: Dict[str, Any]={}
    ) -> List[ActionHook]:
        
        actions = await self.load_data()

        return await asyncio.gather(*[
            self.parser.parse(
                action_data,
                self.stage,
                self.parser_config,
                options
            ) for action_data in actions
        ])
    
    async def load_data(
        self, 
        options: Dict[str, Any]={}
    ) -> List[Dict[str, Any]]:
        data: Dict[str, List[ConsumerRecord]] = await self._consumer.getmany(
            timeout_ms=self.timeout,
            max_records=options.get('max_records')
        )

        records: List[Dict[str, Any]] = []

        for messages in data.values():
            for message in messages:
                records.append(
                    json.loads(message.value)
                )

        return records


    async def close(self):
        await self._consumer.stop()