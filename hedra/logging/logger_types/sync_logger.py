import sys
import logging
from typing import Mapping, Any, List
from aiologger.levels import LogLevel


class SyncLogger(logging.Logger):

    def __init__(
        self, 
        name: str, 
        level: LogLevel = LogLevel.INFO, 
        logger_enabled: bool=True
    ) -> None:
        super().__init__(name, level=level)
        self.logger_enabled = logger_enabled

    def initialize(self, pattern: str, datefmt_pattern: str=None):
        stream_handler = logging.StreamHandler(stream=sys.stdout)
        self.setLevel(self.level)
        stream_handler.setFormatter(
            logging.Formatter(pattern, datefmt=datefmt_pattern)
        )

        self.addHandler(stream_handler)


    def _log(self, level: int, msg: object, *args: List[Any], **kwargs: Mapping[str, Any]) -> None:
        if self.logger_enabled:
            return super()._log(level, msg, *args, **kwargs)