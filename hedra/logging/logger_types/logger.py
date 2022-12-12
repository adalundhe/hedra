import datetime
from aiologger.levels import LogLevel
from aiologger.handlers.files import RolloverInterval   
from .async_logger import AsyncLogger
from .sync_logger import SyncLogger
from .logger_types import LoggerTypes, LoggerTypesMap


class Logger:

    def __init__(
        self, 
        logger_name: str, 
        logger_type: LoggerTypes,
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


        if logger_type == LoggerTypes.FILESYSTEM:
            self.aio = self.logger_types.async_loggers.get(logger_type, AsyncLogger)(
                log_level=log_level,
                logger_enabled=logger_enabled,
                rotation_interval_type=rotation_interval_type,
                rotation_interval=rotation_interval,
                backups=backup_count,
                rotation_time=rotation_time
            )

            self.sync = self.logger_types.sync_loggers.get(logger_type, SyncLogger)(
                log_level=log_level,
                logger_enabled=logger_enabled,
                rotation_interval_type=rotation_interval_type,
                rotation_interval=rotation_interval,
                backups=backup_count,
                rotation_time=rotation_time
            )

        else:
            self.aio = self.logger_types.async_loggers.get(logger_type, AsyncLogger)(
                logger_name,
                level=log_level,
                logger_enabled=logger_enabled
            )

            self.sync = self.logger_types.sync_loggers.get(logger_type, SyncLogger)(
                logger_name,
                level=log_level,
                logger_enabled=logger_enabled
            )

    def set_patterns(self, pattern: str, datefmt_pattern: str=None):
        self.aio.initialize(
            pattern,
            datefmt_pattern=datefmt_pattern
        )

        self.sync.initialize(
            pattern,
            datefmt_pattern=datefmt_pattern
        )