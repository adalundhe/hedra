import os
import datetime
from typing import Dict, Any
from aiologger.handlers.files import RolloverInterval
from .logger_types import Logger, LoggerTypes
from .logging_manager import logging_manager


class HedraLogger:

    def __init__(self) -> None:
        self.loggers: Dict[str, Logger] = {}

        self.logger_names = logging_manager.logger_types.names
        self.logger_types = logging_manager.logger_types.types

    def initialize(
        self, 
        rotation_interval_type: RolloverInterval=RolloverInterval.DAYS,
        rotation_interval: int=1,
        backups: int=1,
        rotation_time: datetime.time=None
    ):

        for logger_type in self.logger_types:
            
            logger_name = logging_manager.logger_types.get_name(logger_type)
            logger_enabled = logging_manager.get_logger_enabled_state(logger_type)


            logger = Logger(
                logger_name,
                logger_type,
                log_level=logging_manager.log_level,
                logger_enabled=logger_enabled,
                rotation_interval_type=rotation_interval_type,
                rotation_interval=rotation_interval,
                backup_count=backups,
                rotation_time=rotation_time
            )

            if logger_type == LoggerTypes.CONSOLE:
                logger.set_patterns('%(message)s')

            elif logger_type == LoggerTypes.DISTRIBUTED or logger_type == LoggerTypes.HEDRA:

                logger.set_patterns(
                    '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(funcName)s:%(lineno)d - Thread: %(thread)d Process:%(process)d - %(message)s',
                    datefmt_pattern='%Y-%m-%dT%H:%M:%S.Z'
                )

            self.loggers[logger_name] = logger

    def __getitem__(self, logger_name: str) -> Logger:
        logger = self.loggers.get(logger_name)
        if logger is None:
            logger = Logger(
                logger_name,
                LoggerTypes.CONSOLE,
                log_level=logging_manager.log_level
            )

            logger.set_patterns('%(message)s')

        return logger

    def create_logger(
        self,
        logger_name: str,
        logger_type: LoggerTypes,
        rotation_interval_type: RolloverInterval=RolloverInterval.DAYS,
        rotation_interval: int=1,
        backups: int=1,
        rotation_time: datetime.time=None
    ):

        logger_enabled = logging_manager.get_logger_enabled_state(logger_type)

        self.loggers[logger_name] = Logger(
            logger_name,
            logger_type,
            log_level=logging_manager.log_level,
            logger_enabled=logger_enabled,
            rotation_interval_type=rotation_interval_type,
            rotation_interval=rotation_interval,
            backup_count=backups,
            rotation_time=rotation_time
        )