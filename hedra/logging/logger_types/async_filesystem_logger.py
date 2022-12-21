import os
import asyncio
import datetime
from typing import Dict
from pathlib import Path
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
from hedra.tools.filesystem import open
from .handers.async_file_handler import AsyncTimedRotatingFileHandler, RolloverInterval
from .logger_types import LoggerTypes
from .async_logger import AsyncLogger

class AsyncFilesystemLogger:

    def __init__(
        self, 
        logger_name: str=None,
        logger_type: LoggerTypes=LoggerTypes.FILESYSTEM,
        logfiles_directory: str=None,
        log_level: LogLevel=LogLevel.NOTSET, 
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

        self.files: Dict[str, AsyncLogger] = {}
        self.filepaths: Dict[str, str] = {}

    def __getitem__(self, logger_name: str):

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

    def _create_file_logger(self, logger_name: str, filepath: str) -> AsyncLogger:
        async_logger = AsyncLogger(
            logger_name=logger_name,
            logger_type=self.logger_type,
            log_level=self.log_level,
            logger_enabled=self.logger_enabled
        )

        async_file_handler = AsyncTimedRotatingFileHandler(
            filepath,
            when=self.rotation_interval_type,
            interval=self.rotation_interval,
            backup_count=self.backups,
            at_time=self.rotation_time
        )

        async_file_handler.formatter = Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - %(message)s',
            '%Y-%m-%dT%H:%M:%S.%Z'
        )

        async_logger.add_handler(async_file_handler)

        return async_logger

    async def create_logfile(self, log_filename: str):

        filepath = os.path.join(self.logfiles_directory, log_filename)

        loop = asyncio.get_event_loop()
        path_exists = await loop.run_in_executor(
            None,
            os.path.exists,
            filepath
        )
        
        if path_exists is False:
            logfile = await open(filepath, 'w')
            await logfile.close()

            self.update_files(filepath)

        else:
            self.update_files(filepath)

    def update_files(self, filepath: str):
        logger_name = Path(filepath).stem

        if self.files.get(logger_name) is None:
            self.files[logger_name] = self._create_file_logger(logger_name, filepath)
            self.filepaths[logger_name] = filepath

