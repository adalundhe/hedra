import asyncio
import uuid
from typing import (
    List, 
    Dict, 
    Union,
    Any,
    Coroutine
)
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.data.parsers.parser import Parser
from .sqlite_connector_config import SQLiteConnectorConfig


def noop_create_async_engine():
    pass


try:
    import sqlalchemy
    from sqlalchemy.engine.result import Result as SQLResult
    from sqlalchemy.ext.asyncio import (
        create_async_engine,
        AsyncEngine,
        AsyncConnection,
    )
        
    has_connector = True

except Exception:
    sqlalchemy = object
    SQLResult = None
    create_async_engine = noop_create_async_engine
    AsyncEngine = object
    AsyncConnection = object
    has_connector = False



class SQLiteConnector:
    connector_type=ConnectorType.SQLite

    def __init__(
        self, 
        config: SQLiteConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.path = f'sqlite+aiosqlite:///{config.path}'
        self.table_name = config.table_name
        self.stage = stage
        self.parser_config = parser_config

        self.metadata = sqlalchemy.MetaData()

        self.database = None
        self._engine: Union[AsyncEngine, None] = None
        self._connection: Union[AsyncConnection, None] = None

        self._table: Union[sqlalchemy.Table, None] = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.parser = Parser()

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to SQLite at - {self.path} - Database: {self.database}')
        self._engine = create_async_engine(self.path)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to SQLite at - {self.path} - Database: {self.database}')

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
        
        if self._table is None:
            self._table = sqlalchemy.Table(
                self.table_name,
                self.metadata,
                autoload_with=self._engine
            )

        async with self._engine.connect() as connection:
            connection: AsyncConnection = connection
            results: SQLResult = await self._connection.execute(
                self._table.select(**options)
            )
            
            return [
                {
                    column: value  for column, value in result._mapping.items()
                } for result in results.fetchall()
            ]
    
    async def close(self):
        await self._connection.close()