import datetime
from typing import Dict
from aiologger.levels import LogLevel
from aiologger.handlers.files import RolloverInterval   
from typing import Generic, TypeVar
from .async_filesystem_logger import AsyncFilesystemLogger
from .sync_filesystem_logger import SyncFilesystemLogger
from .async_logger import AsyncLogger
from .sync_logger import SyncLogger
from .logger_types import LoggerTypes, LoggerTypesMap

A = TypeVar('A')
S = TypeVar('S')


class Logger(Generic[A, S]):

    def __init__(
        self, 
        logger_name: str, 
        logger_type: LoggerTypes,
        logfiles_directory: str=None,
        log_level: LogLevel=LogLevel.INFO, 
        logger_enabled: bool=None, 
        rotation_interval_type: RolloverInterval=RolloverInterval.DAYS,
        rotation_interval: int=1,
        backup_count: int=1,
        rotation_time: datetime.time=None
    ) -> None:
        self.logger_types = LoggerTypesMap()
        self.log_level = log_level
        self.logger_enabled = logger_enabled


        if logger_type == LoggerTypes.FILESYSTEM or logger_type == LoggerTypes.DISTRIBUTED_FILESYSTEM:
            self.aio: A = self.logger_types.async_loggers.get(logger_type, AsyncFilesystemLogger)(
                logfiles_directory,
                log_level=log_level,
                logger_enabled=logger_enabled,
                rotation_interval_type=rotation_interval_type,
                rotation_interval=rotation_interval,
                backups=backup_count,
                rotation_time=rotation_time
            )

            self.sync: S = self.logger_types.sync_loggers.get(logger_type, SyncFilesystemLogger)(
                logfiles_directory,
                log_level=log_level,
                logger_enabled=logger_enabled,
                rotation_interval_type=rotation_interval_type,
                rotation_interval=rotation_interval,
                backups=backup_count,
                rotation_time=rotation_time
            )

        else:
            self.aio: A = self.logger_types.async_loggers.get(logger_type, AsyncLogger)(
                logger_name,
                level=log_level,
                logger_enabled=logger_enabled
            )

            self.sync: S = self.logger_types.sync_loggers.get(logger_type, SyncLogger)(
                logger_name,
                level=log_level,
                logger_enabled=logger_enabled
            )

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

