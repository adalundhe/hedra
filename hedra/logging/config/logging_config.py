import os
import datetime
import signal
from typing import Any, List, Dict, Coroutine
from aiologger.levels import LogLevel
from hedra.logging.logger_types.handers.async_file_handler import RolloverInterval
from yaspin.spinners import Spinners
from hedra.logging.logger_types.logger_types import LoggerTypes
from hedra.logging.spinner import ProgressText


class LoggingConfig:
    logger_name: str=None
    logger_type: LoggerTypes=LoggerTypes.CONSOLE
    logfiles_directory: str=f'{os.getcwd()}/logs'
    log_level: LogLevel=LogLevel.INFO
    logger_enabled: bool=None
    filesystem_rotation_interval_type: RolloverInterval=RolloverInterval.DAYS
    filesystem_rotation_interval: int=1
    filesystem_backup_count: int=1
    filesystem_rotation_time: datetime.time=None
    spinner_type: Spinners=Spinners.bouncingBar
    spinner_color: str='cyan'
    spinner_on_color: str=None
    spinner_attrs: List[str]=["bold"]
    spinner_reversal: bool=False
    spinner_side: str="left"
    spinner_sigmap: Dict[signal.Signals, Coroutine]=None
    spinner_has_timer: bool=False
    spinner_enabled: bool=True
    spinner_display: ProgressText=None

    def from_dict(self, config: Dict[str, Any]):
        for config_value_name, config_value in config.items():
            if hasattr(self, config_value_name):
                setattr(self, config_value_name, config_value)

    @property
    def filesystem_logger(self):
        return {
            'logger_name': self.logger_name,
            'logger_type': self.logger_type,
            'logfiles_directory': self.logfiles_directory,
            'log_level': self.log_level,
            'logger_enabled': self.logger_enabled,
            'rotation_interval_type': self.filesystem_rotation_interval_type,
            'rotation_interval': self.filesystem_rotation_interval,
            'backup_count': self.filesystem_backup_count,
            'rotation_time': self.filesystem_rotation_time
        }

    @property
    def cli_logger(self):
        return {
            'logger_name': self.logger_name,
            'logger_type': self.logger_type,
            'log_level': self.log_level,
            'logger_enabled': self.logger_enabled,
        }

    @property
    def spinner(self):
        return {
            'logger_name': self.logger_name,
            'logger_type': self.logger_type,
            'log_level': self.log_level,
            'logger_enabled': self.logger_enabled,
            'spinner': self.spinner_type,
            'color': self.spinner_color,
            'on_color': self.spinner_on_color,
            'attrs': self.spinner_attrs,
            'reversal': self.spinner_reversal,
            'side': self.spinner_side,
            'sigmap': self.spinner_sigmap,
            'timer': self.spinner_has_timer,
            'enabled': self.spinner_enabled,
            'text': self.spinner_display
        }