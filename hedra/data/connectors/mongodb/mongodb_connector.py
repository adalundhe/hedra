import asyncio
import uuid
from typing import (
    List, 
    Dict, 
    Any,
    Coroutine
)
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.data.parsers.parser import Parser
from hedra.logging import HedraLogger
from .mongodb_connector_config import MongoDBConnectorConfig


try:
    from motor.motor_asyncio import AsyncIOMotorClient
    has_connector = True

except Exception:
    AsyncIOMotorClient = None
    has_connector = False


class MongoDBConnector:
    connector_type=ConnectorType.MongoDB

    def __init__(
        self, 
        config: MongoDBConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.host = config.host
        self.username = config.username
        self.password = config.password
        self.database_name = config.database
        self.stage = stage
        self.parser_config = parser_config

        self.collection = config.collection
        self.connection: AsyncIOMotorClient = None
        self.database = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()
        self.parser = Parser()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to MongoDB instance at - {self.host} - Database: {self.database_name}')

        if self.username and self.password:
            connection_string = f'mongodb://{self.username}:{self.password}@{self.host}/{self.database_name}'
        
        else:
            connection_string = f'mongodb://{self.host}/{self.database_name}'

        self.connection = AsyncIOMotorClient(connection_string)
        self.database = self.connection[self.database_name]

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to MongoDB instance at - {self.host} - Database: {self.database_name}')

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
        
        actions: List[Dict[str, Any]] = await self.load_data(
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
        return await self.database[self.collection].find(
            limit=options.get('limit')
        )
    
    async def close(self):
        await self.connection.close()

