import asyncio
import uuid
import functools
import signal
import psutil
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
from concurrent.futures import ThreadPoolExecutor
from .s3_connector_config import S3ConnectorConfig

try:
    import boto3
    has_connector = True

except Exception:
    boto3 = None
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


class S3Connector:
    connector_type=ConnectorType.S3

    def __init__(
        self, 
        config: S3ConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.bucket_name = config.bucket_name
        self.stage = stage
        self.parser_config = parser_config

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self.client = None
        self._loop = asyncio.get_event_loop()

        self._bucket = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.parser = Parser()

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to AWS S3 - Region: {self.region_name}')

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop
                )
            )

        self.client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                's3',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to AWS S3 - Region: {self.region_name}')
    
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
        keys_data: Dict[str, List[Dict[str, Any]] ] = await self._loop.run_in_executor(
            self._executor,        
            functools.partial(
                self.client.list_objects_v2,
                Bucket=self.bucket_name
            )
        )

        keys: List[str] = [
            item.get('Key') for item in keys_data.get('Contents')
        ]

        return await asyncio.gather(*[
            self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self.client.get_object,
                    Bucket=self.bucket_name,
                    Key=key
                    
                )
            ) for key in keys
        ])
    
    async def close(self):
        self._executor.shutdown(
            wait=False, 
            cancel_futures=True
        )
