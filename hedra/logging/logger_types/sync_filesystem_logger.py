import os
import datetime
from pathlib import Path
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
from logging.handlers import TimedRotatingFileHandler
from aiologger.handlers.files import RolloverInterval
from .sync_logger import SyncLogger


class SyncFilesystemLogger:

    def __init__(
        self, 
        log_level=LogLevel.NOTSET, 
        logger_enabled: bool = True,
        rotation_interval_type: RolloverInterval=RolloverInterval.DAYS,
        rotation_interval: int=1,
        backups: int=1,
        rotation_time: datetime.time=None
    ) -> None:
        self.log_level = log_level
        self.logger_enabled = logger_enabled
        self.rotation_interval_type = rotation_interval_type
        self.rotation_interval = rotation_interval
        self.backups = backups
        self.rotation_time = rotation_time

        self.files = {}

    def __getitem__(self, filepath: str=None):

        file_logger = self.files.get(filepath)
        if file_logger is None and isinstance(filepath, str):

            logger_name = Path(filepath).stem

            sync_logger = SyncLogger(
                name=logger_name,
                level=self.log_level,
                logger_enabled=self.logger_enabled
            )

            sync_file_handler = TimedRotatingFileHandler(
                filepath,
                when=self.rotation_interval_type,
                interval=self.rotation_interval,
                backup_count=self.backups,
                at_time=self.rotation_time
            )

            sync_file_handler.formatter = Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - Thread: %(thread)d Process:%(process)d - %(message)s',
                '%Y-%m-%dT%H:%M:%S.Z'
            )

            sync_logger.addHandler(sync_file_handler)

        else:
            return file_logger

    def create_logfile(self, filepath: str):
        if os.path.exists(filepath) is False:
            log_file = open(filepath, 'w')
            log_file.close()
