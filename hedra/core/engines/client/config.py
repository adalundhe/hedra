import psutil
from typing import List
from .time_parser import TimeParser


class Config:
    
    def __init__(self, **kwargs) -> None:

        for config_option_name, config_option_value in dict(kwargs).items():
            if config_option_value is None:
                del kwargs[config_option_name]
    
        

        self.total_time_string = kwargs.get('total_time', '1m')
        parsed_time = TimeParser(self.total_time_string)

        self.log_level = kwargs.get('log_level', 'info')
        self.persona_type = kwargs.get('persona_type', 'default')
        self.total_time = parsed_time.time
        self.batch_size = kwargs.get('batch_size', 1000)
        self.batch_interval = kwargs.get('batch_interval')
        self.action_interval = kwargs.get('action_interval', 0)
        self.optimize_iterations = kwargs.get('optimize_iterations', 0)
        self.optimizer_type = kwargs.get('optimizer_type', 'shg')
        self.batch_gradient = kwargs.get('batch_gradient', 0.1)
        self.cpus = kwargs.get('cpus', psutil.cpu_count(logical=False))
        self.no_run_visuals = kwargs.get('no_run_visuals', False)
        self.connect_timeout = kwargs.get('connect_timeout', 15)
        self.request_timeout = kwargs.get('request_timeout', 60)
        self.reset_connections = kwargs.get('reset_connections')
        self.graceful_stop = kwargs.get('graceful_stop', 1)
        self.optimized = False

        if self.request_timeout > self.total_time:
            self.request_timeout = self.total_time

        self.browser_type = kwargs.get('browser_type', 'chromium')
        self.device_type = kwargs.get('device_type')
        self.locale = kwargs.get('locale')
        self.geolocation = kwargs.get('geolocation')
        self.permissions: List[str] = kwargs.get('permissions', [])
        self.color_scheme = kwargs.get('color_scheme')
        self.group_size = kwargs.get('group_size')
        self.playwright_options = kwargs.get('playwright_options', {})
