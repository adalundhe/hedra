import asyncio
import json
import time
import uuid
from typing import (
    List, 
    Dict, 
    Any,
    Coroutine
)
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.data.parsers.parser import Parser
from .redis_connector_config import RedisConnectorConfig


try:

    import aioredis
    
    has_connector = True

except Exception:
    aioredis = None
    has_connector = True


class RedisConnector:
    connector_type=ConnectorType.Redis

    def __init__(
        self, 
        config: RedisConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.host = config.host
        self.base = 'rediss' if config.secure else 'redis'
        self.username = config.username
        self.password = config.password
        self.database = config.database
        self.channel = config.channel
        self.stage = stage
        self.parser_config = parser_config

        self.channel_type = config.channel_type
        self.connection = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.parser = Parser()

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Redis instance at - {self.base}://{self.host} - Database: {self.database}')
        
        self.connection = await aioredis.from_url(
            f'{self.base}://{self.host}',
            username=self.username,
            password=self.password,
            db=self.database
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Redis instance at - {self.base}://{self.host} - Database: {self.database}')

    async def load_execute_stage_summary(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, ExecuteStageSummaryValidator]:
        execute_stage_summary = await self.load_data(
            options=options
        )
        
        return ExecuteStageSummaryValidator(**execute_stage_summary)

    async def load_actions(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, List[ActionHook]]:
        actions = await self.load_data(
            options=options
        )

        return await asyncio.gather(*[
            self.parser.parse_action(
                action_data,
                self.stage,
                self.parser_config,
                options
            ) for action_data in actions
        ])
    
    async def load_results(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, ResultsSet]:
        results = await self.load_data(
            options=options
        )

        return ResultsSet({
            'stage_results': await asyncio.gather(*[
                self.parser.parse_result(
                    results_data,
                    self.stage,
                    self.parser_config,
                    options
                ) for results_data in results
            ])
        })
    
    async def load_data(
        self, 
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, List[Dict[str, Any]]]:
        
        if self.channel:
            async with self.connection.client() as connection:
                keys = await connection.keys()
                return await asyncio.gather(*[
                    asyncio.create_task(
                        connection.get(key) for key in keys
                    )
                ])
            
        else:
            subscriber = self.connection.pubsub()
            await subscriber.subscribe(self.channel)

            limit = options.get('limit')
            timeout = options.get('timeout', 1)

            if limit is None and timeout is None:
                raise Exception('A limit or timeout must be provided.')
            
            if limit:

                async with subscriber as subscription:
                    results: List[bytes] = await asyncio.gather(*[
                        asyncio.create_task(subscription.get_message(
                            ignore_subscribe_messages=True
                        )) for _ in range(limit)
                    ])
                
            else:
                async with subscriber as subscription:
                    elapsed = 0
                    start = time.time()

                    results: List[bytes] = []

                    while elapsed < timeout:
                        results.append(
                            await subscription.get_message(
                                ignore_subscribe_messages=True
                            )
                        )

                        elapsed = time.time() - start

            return [
                json.loads(result) for result in results
            ]  
    
    async def close(self):
        await self.connection.close()
