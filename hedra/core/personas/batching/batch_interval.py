
from hedra.core.hooks.client.config import Config


class BatchInterval:
    
    def __init__(self, config: Config) -> None:
        self.period = config.batch_interval
