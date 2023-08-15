import asyncio
import uuid
import functools
import signal
import psutil
from concurrent.futures import ThreadPoolExecutor
from typing import (
    List, 
    Dict, 
    Any,
    Coroutine
)
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.data.parsers.parser import Parser
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.logging import HedraLogger
from .bigtable_connector_config import BigTableConnectorConfig

try:
    from google.cloud import bigtable
    from google.auth import load_credentials_from_file
    from google.cloud.bigtable.row_data import PartialRowsData
    has_connector = True

except Exception:
    bigtable = None
    Credentials = None
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


class BigTableConnector:
    connector_type = ConnectorType.BigTable

    def __init__(
        self, 
        config: BigTableConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:

        self.service_account_json_path = config.service_account_json_path
        self.instance_id = config.instance_id
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self.instance = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.stage = stage
        self.parser_config = parser_config

        self.logger = HedraLogger()
        self.logger.initialize()

        self._table_name = config.table_name
        self._column_family_name = None
        self._table = None  
        self._columns = None
    
        self.credentials = None
        self.client = None
        self._loop = asyncio.get_event_loop()

        self.parser = Parser()

    async def connect(self):

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to Google Cloud - Loading account config from - {self.service_account_json_path}')
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')

        credentials, project_id = load_credentials_from_file(self.service_account_json_path)
        self.client = bigtable.Client(
            project=project_id,
            credentials=credentials,
            admin=True
        )
        self.instance = self.client.instance(self.instance_id)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opened connection to Google Cloud - Created Client Instance - ID:{self.instance_id}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opened connection to Google Cloud - Loaded account config from - {self.service_account_json_path}')

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
        self._table = self.instance.table(self._table_name)

        data_rows: PartialRowsData = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._table.read_rows
            )
        )

        return [
            row.to_dict() for row in data_rows
        ]
    
    async def close(self):
        self._executor.shutdown(cancel_futures=True)
