import os
import sys
import asyncio
import aiologger
from asyncio import Task
from aiologger.formatters.base import Formatter
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.levels import (
    LogLevel
)


class AsyncLogger(aiologger.Logger):

    def __init__(
        self, 
        name: str, 
        level: LogLevel = LogLevel.INFO, 
        logger_enabled: bool=True
    ) -> None:
        super().__init__(name=name, level=level)

        self.logger_enabled = logger_enabled

    def initialize(self, pattern: str, datefmt_pattern: str=None):
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

    async def _skip_task(self, level, msg, *args, **kwargs) -> Task:
        return


