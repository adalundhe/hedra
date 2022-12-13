from __future__ import annotations
import os
import sys
import asyncio
import aiologger
from asyncio import Task
from aiologger.formatters.base import Formatter
from aiologger.handlers.streams import AsyncStreamHandler
from aiologger.levels import LogLevel
from hedra.logging.graphics import cli_progress_manager, AsyncSpinner


class AsyncLogger(aiologger.Logger):

    def __init__(
        self, 
        name: str, 
        level: LogLevel = LogLevel.INFO, 
        logger_enabled: bool=True
    ) -> None:
        super().__init__(name=name, level=level)

        self.logger_enabled = logger_enabled
        self.progress_manager = cli_progress_manager
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

    def create_spinner(
        self,
        spinner=None, 
        text="", 
        color=None, 
        on_color=None, 
        attrs=None, 
        reversal=False, 
        side="left", 
        sigmap=None, 
        timer=False,
        logger: AsyncLogger=None
    ) -> AsyncSpinner:
        spinner = AsyncSpinner(
            spinner=spinner, 
            text=text, 
            color=color, 
            on_color=on_color, 
            attrs=attrs, 
            reversal=reversal, 
            side=side, 
            sigmap=sigmap, 
            timer=timer,
            enabled=self.logger_enabled
        )

        if logger is None:
            logger = self

        spinner.set_logger(
            logger, 
            logger_enabled=logger.logger_enabled
        )

        return spinner

    def progress(self, message: str):
        return self.progress_manager.spinner.write(message)

    def append_progress_message(self, message: str):
        return self.progress_manager.append_cli_message(message)

    def set_default_message(self, message: str):
        return self.progress_manager.clear_and_replace(message)

    def set_progress_ok(self, message: str):
        return self.progress_manager.spinner.ok(message)

    def set_progress_fail(self, message: str):
        return self.progress_manager.spinner.fail(message)

    def _make_log_task(self, level, msg, *args, **kwargs) -> Task:
        if self.logger_enabled:
            return super()._make_log_task(level, msg, *args, **kwargs)

        else:
            return asyncio.create_task(self._skip_task())

    async def _skip_task(self):
        return


