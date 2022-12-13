from typing import Dict
from typing import Generic, TypeVar
from hedra.logging.config import LoggingConfig
from .async_filesystem_logger import AsyncFilesystemLogger
from .async_logger import AsyncLogger
from .async_spinner import AsyncSpinner
from .sync_filesystem_logger import SyncFilesystemLogger
from .sync_logger import SyncLogger
from .logger_types import LoggerTypes
from .logger_types_map import LoggerTypesMap


A = TypeVar('A')
S = TypeVar('S')


class Logger(Generic[A, S]):

    def __init__(self, config: LoggingConfig) -> None:
        self.logger_types = LoggerTypesMap()
        self.log_level = config.log_level
        self.logger_enabled = config.logger_enabled

        if config.logger_type == LoggerTypes.SPINNER:
            self.aio: A = self.logger_types.async_loggers.get(config.logger_type, AsyncSpinner)(**config.spinner)
            self.sync: S = None

        elif config.logger_type == LoggerTypes.FILESYSTEM or config.logger_type == LoggerTypes.DISTRIBUTED_FILESYSTEM:
            self.aio: A = self.logger_types.async_loggers.get(config.logger_type, AsyncFilesystemLogger)(**config.filesystem_logger)
            self.sync: S = self.logger_types.sync_loggers.get(config.logger_type, SyncFilesystemLogger)(**config.filesystem_logger)

        else:
            self.aio: A = self.logger_types.async_loggers.get(config.logger_type, AsyncLogger)(**config.cli_logger)
            self.sync: S = self.logger_types.sync_loggers.get(config.logger_type, SyncLogger)(**config.cli_logger)

    def set_patterns(self, pattern: str, datefmt_pattern: str=None):

        if isinstance(self.aio, AsyncLogger) and isinstance(self.sync, SyncLogger):
            self.aio.initialize(
                pattern,
                datefmt_pattern=datefmt_pattern
            )

            self.sync.initialize(
                pattern,
                datefmt_pattern=datefmt_pattern
            )

    def create_filelogger(self, filepath: str):
        if isinstance(self.aio, AsyncFilesystemLogger) and isinstance(self.sync, SyncFilesystemLogger):
            self.aio.update_files(filepath)
            self.sync.update_files(filepath)

    @property
    def files(self) -> Dict[str, str]:
        
        logfiles = {}
        if isinstance(self.aio, AsyncFilesystemLogger) and isinstance(self.sync, SyncFilesystemLogger):
            logfiles.update(self.aio.filepaths)
            logfiles.update(self.sync.filepaths)

        return logfiles

