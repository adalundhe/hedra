# # This is an ugly patch for: https://github.com/aio-libs/aiopg/issues/837
# import selectors  # isort:skip # noqa: F401

# selectors._PollLikeSelector.modify = (  # type: ignore
#     selectors._BaseSelectorImpl.modify  # type: ignore
# )
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
from .postgres_connector_config import PostgresConnectorConfig


try:
    import sqlalchemy
    from sqlalchemy.engine.result import Result as SQLResult
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.ext.asyncio.engine import (
        AsyncEngine, 
        AsyncConnection
    )

    has_connector = True

except Exception:
    SQLResult = object
    sqlalchemy = object
    AsyncEngine = object
    AsyncConnection = object
    AsyncTransaction = object
    create_async_engine: lambda *args, **kwargs: None
    has_connector = False


class PostgresConnection:
    connection_type=ConnectorType.Postgres

    def __init__(
        self, 
        config: PostgresConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        
        self.host = config.host
        self.database = config.database
        self.username = config.username
        self.password = config.password
        self.stage = stage
        self.parser_config = parser_config

        self.table_name = config.table_name
        
        self._engine: Union[AsyncEngine, None] = None
        self.metadata = sqlalchemy.MetaData()

        self._table = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()
        
        self.parser = Parser()
        self.sql_type = 'Postgresql'

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to {self.sql_type} instance at - {self.host} - Database: {self.database}')

        connection_uri = 'postgresql+asyncpg://'

        if self.username and self.password:
            connection_uri = f'{connection_uri}{self.username}:{self.password}@'

        self._engine: AsyncEngine = await create_async_engine(
            f'{connection_uri}{self.host}/{self.database}',
            echo=False
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to {self.sql_type} instance at - {self.host} - Database: {self.database}')

    async def load_execute_stage_summary(
        self,
        options: Dict[str, Any]={}
    ) -> ExecuteStageSummaryValidator:
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
            
            results: SQLResult = await connection.execute(
                self._table.select(**options)
            )

            return [
                {
                    column: value for column, value in row._mapping.items()
                } for row in results.fetchall()
            ]

    async def close(self):
        pass