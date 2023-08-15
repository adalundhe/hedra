from __future__ import annotations
import asyncio
import functools
import json
import psutil
import uuid
import os
import signal
import pathlib
import re
from typing import (
    List, 
    TextIO, 
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
from .json_connector_config import JSONConnectorConfig


has_connector = True


def handle_loop_stop(
    signame, 
    executor: ThreadPoolExecutor, 
    loop: asyncio.AbstractEventLoop, 
    json_file: TextIO
): 
    try:
        json_file.close()
        executor.shutdown(wait=False, cancel_futures=True) 
        loop.stop()
    except Exception:
        pass


class JSONConnector:
    connector_type=ConnectorType.JSON

    def __init__(
        self, 
        config: JSONConnectorConfig,
        stage: str,
        parser_config: Config,
    ) -> None:
        self.filepath = config.filepath
        
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop: asyncio.AbstractEventLoop = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.stage = stage
        self.parser_config = parser_config

        self.logger = HedraLogger()
        self.logger.initialize()

        self.json_file: TextIO = None

        self.file_mode = config.file_mode
        self.pattern = re.compile("_copy[0-9]+")
        
        self.parser = Parser()

    async def connect(self):
        self._loop = asyncio._get_running_loop()
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Setting filepaths')
        
        if self.filepath[:2] == '~/':
            user_directory = pathlib.Path.home()
            self.filepath = os.path.join(
                user_directory,
                self.filepath[2:]
            )

        self.filepath = await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                os.path.abspath,
                self.filepath
            )
        )
        
        if self.json_file is None:
            self.json_file = await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    open,
                    self.filepath,
                    self.file_mode
                )
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._executor,
                        self._loop,
                        self.json_file
                    )
                )

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Opening from file - {self.filepath}')

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
    ) -> Coroutine[Any, Any, Any]:
        return await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                json.load,
                self.json_file
            )
        )
    
    async def close(self):

        await self._loop.run_in_executor(
            self._executor,
            self.json_file.close
        )

        self._executor.shutdown(cancel_futures=True)     