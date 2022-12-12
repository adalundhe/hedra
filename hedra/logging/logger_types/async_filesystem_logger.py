import os
import asyncio
import datetime
import aiofiles
from pathlib import Path
from aiologger.levels import LogLevel
from aiologger.formatters.base import Formatter
from aiologger.handlers.files import AsyncTimedRotatingFileHandler, RolloverInterval
from .async_logger import AsyncLogger

class AsyncFilesystemLogger:

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
        if file_logger is None:

            logger_name = Path(filepath).stem

            async_logger = AsyncLogger(
                name=logger_name,
                level=self.log_level,
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
                '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - Thread: %(thread)d Process:%(process)d - %(message)s',
                '%Y-%m-%dT%H:%M:%S.Z'
            )

            async_logger.add_handler(async_file_handler)

        else:
            return file_logger

    async def create_logfile(self, filepath: str):

        loop = asyncio.get_event_loop()
        path_exists = await loop.run_in_executor(
            None,
            os.path.exists,
            filepath
        )
        
        if path_exists is False:
            async with aiofiles.open(filepath, 'w') as logfile:
                await logfile.close()
