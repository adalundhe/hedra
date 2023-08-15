import asyncio
import uuid
from typing import (
    List,
    Dict,
    Any,
    Union,
    Coroutine
)
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.data.parsers.parser import Parser
from .cosmos_connector_config import CosmosDBConnectorConfig

try:
    from azure.cosmos.aio import CosmosClient
    from azure.cosmos import DatabaseProxy
    has_connector = True
except Exception:
    CosmosClient = None
    DatabaseProxy = None
    has_connector = False


class CosmosDBConnector:
    connector_type=ConnectorType.CosmosDB

    def __init__(
        self, config: CosmosDBConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.account_uri = config.account_uri
        self.account_key = config.account_key

        self.database_name = config.database
        self.container_name = config.container_name
        self.partition_key = config.partition_key

        self.analytics_ttl = config.analytics_ttl

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.stage = stage
        self.parser_config = parser_config

        self.logger = HedraLogger()
        self.logger.initialize()

        self.container = None
        
        self.client = None
        self.database: Union[DatabaseProxy, None] = None
        self.parser = Parser()

    async def connect(self):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to CosmosDB')

        self.client = CosmosClient(
            self.account_uri,
            credential=self.account_key
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to CosmosDB')
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Creating Database - {self.database_name} - if not exists')
        self.database = self.client.get_database_client(self.database_name)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Created or set Database - {self.database_name}')

        self.container = self.database.get_container_client(self.container_name)
    
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
        actions = await self.load_data()

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
        results = await self.load_data()

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
        return [
            record async for record in self.container.read_all_items(
                max_item_count=options.get('max_item_count'),
            )
        ]
            
    
    async def close(self):
        await self.client.close()
