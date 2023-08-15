import asyncio
import functools
import json
import os
import pathlib
import psutil
import signal
import re
import uuid
from concurrent.futures import ThreadPoolExecutor
from hedra.logging import HedraLogger
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.connectors.common.connector_type import ConnectorType
from hedra.data.parsers.parser import Parser
from typing import (
    Dict, 
    List, 
    TextIO, 
    Union,
    Any,
    Coroutine
)
from .har_connector_config import HARConnectorConfig

try:

    from haralyzer import HarParser, HarEntry
    from haralyzer.http import Request

    has_connector=True

except ImportError:
    has_connector=False

    HarParser=object
    HarEntry=object
    Request=object


def handle_loop_stop(
    signame, 
    executor: ThreadPoolExecutor, 
    loop: asyncio.AbstractEventLoop, 
    harfile: TextIO
): 
    try:
        harfile.close()
        executor.shutdown(wait=False, cancel_futures=True) 
        loop.stop()
    except Exception:
        pass
    

class HARConnector:
    connector_type=ConnectorType.HAR

    def __init__(
        self, 
        config: HARConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.filepath = config.filepath
        self._name_pattern = re.compile('[^0-9a-zA-Z]+')
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.stage = stage
        self.parser_config = parser_config

        self.logger = HedraLogger()
        self.logger.initialize()

        self._parser: Union[HarParser, None] = None
        self.har_file: TextIO = None

        self._loop: asyncio.AbstractEventLoop = None

        self.parser = Parser()
        self._protocol_types_map: Dict[str, str] = {
            "http/3.0": "http3",
            "http/2.0": "http2",
            "http/1.1": "http"
        }

    async def connect(self):
        self._loop = asyncio._get_running_loop()

        if self.filepath[:2] == '~/':
            user_directory = pathlib.Path.home()
            self.filepath = os.path.join(
                user_directory,
                self.filepath[2:]
            )
        
        self.filepath = await self._loop.run_in_executor(
            None,
            functools.partial(
                os.path.abspath,
                self.filepath
            )
        )

        self.har_file = await self._loop.run_in_executor(
            None,
            functools.partial(
                open,
                self.filepath
            )
        )

        for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
            self._loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(
                    signame,
                    self._executor,
                    self._loop,
                    self.har_file
                )
            )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening from file - {self.filepath}')

    async def load_execute_stage_summary(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, NotImplementedError]:
        raise NotImplementedError('Execute stage summary loading is not available for the HAR Connector.')

    async def load_actions(
        self,
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, List[ActionHook]]:
        
        self._parser = HarParser(
            har_data=json.loads(self.har_file)
        )
        
        actions: List[Dict[str, Any]] = []

        action_order = 0

        for page in self._parser.pages:
            page_entries: List[HarEntry] = page.entries

            for entry in page_entries:
                
                page_request: Request = entry.request

                action_data = page_request.text
                
                content_type: str = page_request.mimeType
                if content_type.lower() == 'application/json' and page_request.text:
                    action_data = json.loads(action_data)

                action_url: str = page_request.url
                action_method_stub: str= page_request.method
                action_method = action_method_stub.upper()
                action_headers: List[Dict[str, str]] = page_request.headers

                action_basename = '_'.join([
                    segment.capitalize() for segment in self._name_pattern.sub(
                        '_', 
                        action_url
                    ).split('_')
                ])

                action_fullname = f'{action_method.lower()}_{action_basename}'

                http_type: str = self._protocol_types_map.get(
                    page_request.httpVersion,
                    "http"
                )

                actions.append({
                    'engine': http_type,
                    'name': action_fullname,
                    'url': action_url,
                    'headers': action_headers,
                    'method': action_method,
                    'data': action_data,
                    'order': action_order
                })

                action_order += 1

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
    ) -> Coroutine[Any, Any, NotImplementedError]:
        raise NotImplementedError('Results loading is not available for the HAR Connector.')
    
    async def load_data(
        self, 
        options: Dict[str, Any]={}
    ) -> Coroutine[Any, Any, NotImplementedError]:
        raise NotImplementedError('Data loading is not available for the HAR Connector.')
    
    async def close(self):
        await self._loop.run_in_executor(
            self._executor,
            self.har_file.close
        )

        self._executor.shutdown(cancel_futures=True)
