import psutil
from .time_parser import TimeParser


class Config:
    
    def __init__(self, **kwargs) -> None:

        for config_option_name, config_option_value in dict(kwargs).items():
            if config_option_value is None:
                del kwargs[config_option_name]


        time_parser = TimeParser(
            kwargs.get('total_time', '1m')
        )

        self.log_level=kwargs.get('log_level', 'info')
        self.persona_type=kwargs.get('persona_type', 'simple')
        self.total_time=time_parser.time
        self.batch_size=kwargs.get('batch_size', 1000)
        self.batch_interval=kwargs.get('batch_interval')
        self.optimize_iterations=kwargs.get('optimize_iterations', 0)
        self.optimizer_type=kwargs.get('optimizer_type', 'shg')
        self.batch_gradient=kwargs.get('batch_gradient', 0.1)
        self.cpus=kwargs.get('cpus', psutil.cpu_count(logical=False))
        self.no_run_visuals=kwargs.get('no_run_visuals', False)
        self.connect_timeout=kwargs.get('connect_timeout', 15)
        self.request_timeout=kwargs.get('request_timeout', 60)
        self.reset_connections=kwargs.get('reset_connections')
        self.options={
            **kwargs.get('options', {})
        }
