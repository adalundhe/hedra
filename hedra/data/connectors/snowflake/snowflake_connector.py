import asyncio
import functools
import signal
import uuid
import psutil
from concurrent.futures import ThreadPoolExecutor
from typing import (
    List, 
    Dict, 
    Union,
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
from .snowflake_connector_config import SnowflakeConnectorConfig


def noop_create_engine():
    pass


try:
    import sqlalchemy
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    from sqlalchemy.engine import (
        Engine, 
        Connection
    )
    
    has_connector = True

except Exception:
    sqlalchemy = object
    URL = object
    create_engine=noop_create_engine
    Engine = object
    Connection = object
    has_connector = False


def handle_loop_stop(
    signame, 
    executor: ThreadPoolExecutor, 
    loop: asyncio.AbstractEventLoop
): 
    try:
        executor.shutdown(wait=False, cancel_futures=True) 
        loop.stop()
    except Exception:
        pass


class SnowflakeConnector:
    connector_type=ConnectorType.Snowflake

    def __init__(
        self, 
        config: SnowflakeConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.username = config.username
        self.password = config.password
        self.organization_id = config.organization_id
        self.account_id = config.account_id
        self.private_key = config.private_key
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.database_schema
        self.stage = stage
        self.parser_config = parser_config

        self.table_name = config.table_name

        self.connect_timeout = config.connect_timeout
        
        self.metadata = sqlalchemy.MetaData()
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self._engine: Union[Engine, None] = None
        self._connection: Union[Connection, None] = None

        self._table: Union[sqlalchemy.Table, None] = None

        self._loop = asyncio.get_event_loop()

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()
        
        self.parser = Parser()

    async def connect(self):

        try:

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop
                    )
                )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to Snowflake instance at - Warehouse: {self.warehouse} - Database: {self.database} - Schema: {self.schema}')
            self._engine = await self._loop.run_in_executor(
                self._executor,
                create_engine,
                URL(
                    user=self.username,
                    password=self.password,
                    account=self.account_id,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema
                )
            
            )

            self._connection = await asyncio.wait_for(
                self._loop.run_in_executor(
                    self._executor,
                    self._engine.connect
                ),
                timeout=self.connect_timeout
            )

            await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to Snowflake instance at - Warehouse: {self.warehouse} - Database: {self.database} - Schema: {self.schema}')

        except asyncio.TimeoutError:
            raise Exception('Err. - Connection to Snowflake timed out - check your account id, username, and password.')

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
        
        if self._table is None:
            
            self._table = sqlalchemy.Table(
                self.table_name,
                self.metadata,
                autoload_with=self._engine
            )
            
        results = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._connection.execute,
                self._table.select(**options)
            )
        )
        
        all_results = await self._loop.run_in_executor(
            self._executor,
            results.fetchall
        )

        return [
            {
              column: value  for column, value in result._mapping.items()
            } for result in all_results
        ]
    
    async def close(self):
        await self._loop.run_in_executor(
            self._executor,
            self._connection.close
        )

        self._executor.shutdown()