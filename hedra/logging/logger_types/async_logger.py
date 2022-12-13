from __future__ import annotations
import os
import sys
import asyncio
import aiologger
from asyncio import Task
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
from aiologger.handlers.streams import AsyncStreamHandler
from .logger_types import LoggerTypes


class AsyncLogger(aiologger.Logger):

    def __init__(
        self, 
        logger_name: str=None, 
        logger_type: LoggerTypes=LoggerTypes.CONSOLE,
        log_level: LogLevel = LogLevel.INFO, 
        logger_enabled: bool=True
    ) -> None:
        super().__init__(name=logger_name, level=log_level)

        self.logger_name = logger_name
        self.logger_type = logger_type
        self.log_level = log_level
        self.logger_enabled = logger_enabled
        self.pattern = None
        self.datefmt_pattern = None

    def initialize(self, pattern: str, datefmt_pattern: str=None):

        self.pattern = pattern
        self.datefmt_pattern = datefmt_pattern

        self.add_handler(
            AsyncStreamHandler(
                stream=os.fdopen(
                    os.dup(sys.__stdout__.fileno())
                ),
                level=self.level,
                formatter=Formatter(pattern, datefmt=datefmt_pattern),
            )
        )

    def _make_log_task(self, level, msg, *args, **kwargs) -> Task:

        if self.logger_enabled:
            return super()._make_log_task(level, msg, *args, **kwargs)

        else:
            return asyncio.create_task(self._skip_task())

    async def _skip_task(self):
        return


