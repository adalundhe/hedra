import asyncio
import functools
import uuid
import psutil
import signal
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.parsers.parser import Parser
from typing import List, Dict, Any
from .aws_lambda_connector_config import AWSLambdaConnectorConfig

try:
    import boto3
    has_connector=True
except Exception:
    boto3 = None
    has_connector=False


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


class AWSLambdaConnector:

    def __init__(
        self, 
        config: AWSLambdaConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.aws_access_key_id = config.aws_access_key_id
        self.aws_secret_access_key = config.aws_secret_access_key
        self.region_name = config.region_name
        self.lambda_name = config.lambda_name

        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._client = None
        self._loop = asyncio.get_event_loop()
        self.session_uuid = str(uuid.uuid4())

        self.connector_type = ConnectorType.AWSLambda
        self.connector_type_name = self.connector_type.name.capitalize()
        self.metadata_string: str = None
        self.stage = stage
        self.parser_config = parser_config

        self.logger = HedraLogger()
        self.logger.initialize()

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

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Opening session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening amd authorizing connection to AWS - Region: {self.region_name}')

        self._client = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                boto3.client,
                'lambda',
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name
            )
        )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Successfully opened connection to AWS - Region: {self.region_name}')

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
    ) -> Any:
        return await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._client.invoke,
                FunctionName=self.lambda_name
            )
        )
    
    async def close(self):
        self._executor.shutdown(cancel_futures=True)
