import asyncio
from typing import List
from aiologger.levels import LogLevel
from .spinner import ProgressText
from .logger_types import (
    LoggerTypes,
    LoggerTypesMap
)


class LoggingManager:

    def __init__(self) -> None:

        self.logger_types = LoggerTypesMap()
        self.enabled_loggers = {}

        for logger_type in self.logger_types.async_loggers.keys():
            self.enabled_loggers[logger_type] = True

        self.log_levels = {
            'info': LogLevel.INFO,
            'debug': LogLevel.DEBUG,
            'error': LogLevel.ERROR,
            'warning': LogLevel.WARNING,
            'warn': LogLevel.WARN,
            'critical': LogLevel.CRITICAL,
            'fatal': LogLevel.FATAL
        }

        self.log_level = LogLevel.INFO
        self.log_level_name = 'info'
        self.logfiles_directory = None
        self.progress_display = ProgressText()

    def update_log_level(self, log_level_name: str):
        self.log_level = self.log_levels.get(log_level_name, LogLevel.INFO)
        self.log_level_name = log_level_name

    def get_logger_enabled_state(self, logger_type: LoggerTypes):
        return self.enabled_loggers.get(logger_type)

    def enable(self, *logger_types: List[LoggerTypes]):
        for logger_type in logger_types:
            self.enabled_loggers[logger_type] = True

    def disable(self, *logger_types: List[LoggerTypes]):
        for logger_type in logger_types:
            self.enabled_loggers[logger_type] = False

    def list_enabled(self):
        return [
            logger_type for logger_type, logger_enabled in self.enabled_loggers.items() if logger_enabled
        ]

    def list_disabled(self):
        return [
            logger_type for logger_type, logger_enabled in self.enabled_loggers.items() if not logger_enabled
        ]

    def list_enabled_type_names(self):
        enabled_loggers = self.list_enabled()
        return [
            self.logger_types.get_name(logger_type) for logger_type in enabled_loggers
        ]

    def list_disabled_type_names(self):
        disabled_loggers = self.list_disabled()
        return [
            self.logger_types.get_name(logger_type) for logger_type in disabled_loggers
        ]


logging_manager = LoggingManager()