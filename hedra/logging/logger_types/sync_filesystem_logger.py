import os
import datetime
from logging import Formatter
from typing import Dict
from pathlib import Path
from aiologger.levels import LogLevel
from logging.handlers import TimedRotatingFileHandler
from hedra.logging.logger_types.handers.async_file_handler import RolloverInterval
from .logger_types import LoggerTypes
from .sync_logger import SyncLogger


class SyncFilesystemLogger:

    def __init__(
        self, 
        logger_name: str=None,
        logger_type: LoggerTypes=LoggerTypes.FILESYSTEM,
        log_level: LogLevel=LogLevel.NOTSET, 
        logfiles_directory: str=None,
        logger_enabled: bool = True,
        rotation_interval_type: RolloverInterval=RolloverInterval.DAYS,
        rotation_interval: int=1,
        backup_count: int=1,
        rotation_time: datetime.time=None
    ) -> None:
        self.logger_name = logger_name
        self.logger_type = logger_type
        self.log_level = log_level
        self.logger_enabled = logger_enabled
        self.rotation_interval_type = rotation_interval_type
        self.rotation_interval = rotation_interval
        self.backups = backup_count
        self.rotation_time = rotation_time
        self.logfiles_directory: str = logfiles_directory

        self.files: Dict[str, SyncLogger] = {}
        self.filepaths: Dict[str, str] = {}

    def __getitem__(self, logger_name: str=None):

        file_logger = self.files.get(logger_name)

        if file_logger is None:
            file_logger = self._create_file_logger(
                logger_name,
                os.path.join(
                    self.logfiles_directory,
                    f'{logger_name}.log'
                ) 
            )

            self.files[logger_name] = file_logger

        return file_logger

    def _create_file_logger(self, logger_name: str, filepath: str) -> SyncLogger:
        sync_logger = SyncLogger(
            logger_name=logger_name,
            logger_type=self.logger_type,
            log_level=self.log_level,
            logger_enabled=self.logger_enabled
        )

        sync_file_handler = TimedRotatingFileHandler(
            filepath,
            when=self.rotation_interval_type,
            interval=self.rotation_interval,
            backupCount=self.backups,
            atTime=self.rotation_time
        )

        sync_file_handler.formatter = Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s',
            '%Y-%m-%dT%H:%M:%S.%Z'
        )

        sync_logger.addHandler(sync_file_handler)

        return sync_logger

    def create_logfile(self, log_filename: str):

        filepath = os.path.join(self.logfiles_directory, log_filename)

        if os.path.exists(filepath) is False:      
            log_file = open(filepath, 'w')
            log_file.close()

            self.update_files(filepath)

        else:
            self.update_files(filepath)

    def update_files(self, filepath: str):
        logger_name = Path(filepath).stem

        if self.files.get(logger_name) is None:
            self.files[logger_name] = self._create_file_logger(logger_name, filepath)
            self.filepaths[logger_name] = filepath
            
