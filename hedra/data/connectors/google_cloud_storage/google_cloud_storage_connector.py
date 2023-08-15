import asyncio
import functools
import signal
import psutil
import uuid
from typing import (
    List, 
    Dict, 
    Any,
    Coroutine
)
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.connectors.common.execute_stage_summary_validator import ExecuteStageSummaryValidator
from hedra.data.parsers.parser import Parser
from .google_cloud_storage_connector_config import GoogleCloudStorageConnectorConfig


try:

    from google.cloud import storage
    has_connector = True

except Exception:
    storage = None
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


class GoogleCloudStorageConnector:
    connector_type=ConnectorType.GCS

    def __init__(
        self, 
        config: GoogleCloudStorageConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        
        self.service_account_json_path = config.service_account_json_path
        self.stage = stage
        self.parser_config = parser_config

        self.bucket_namespace = config.bucket_namespace
        self.bucket_name = config.bucket_name
       

        self.credentials = None
        self.client = None

        self._bucket = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
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
        self.client = storage.Client.from_service_account_json(self.service_account_json_path)

        self._bucket = await self._loop.run_in_executor(
            self._executor,
            self.client.create_bucket,
            self.bucket_name
        )

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
        blobs: List[storage.Blob] = await self._loop.run_in_executor(
            self._executor,        
            self._bucket.list_blobs
        )

        return await asyncio.gather(*[
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._bucket.get_blob,
                    blob.name
                )
            ) for blob in blobs
        ])
    
    async def close(self):

        await self._loop.run_in_executor(
            self._executor,
            self.client.close
        )

        self._executor.shutdown(wait=False, cancel_futures=True)
