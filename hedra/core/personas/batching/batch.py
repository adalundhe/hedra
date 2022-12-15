from typing import Dict, Union
from hedra.core.engines.client.config import Config
from .param_type import ParamType


class Batch:

    __slots__ = (
        'gradient',
        'size',
        'interval',
        'deferred'
    )

    def __init__(self, config: Config) -> None:
        self.gradient = config.batch_gradient

        self.size = config.batch_size

        self.interval = config.batch_interval

        self.deferred = []

    def to_params(self):
        return {
            'batch_gradient': {
                'value': self.gradient,
                'type': ParamType.FLOAT
            },
            'batch_size': {
                'value': self.size,
                'type': ParamType.INTEGER
            },
            'batch_interval': {
                'value': self.size,
                'type': ParamType.FLOAT
            }
        }

    def update(self, values: Dict[str, Union[int, float]]):
        gradient = values.get('batch_gradient')
        if gradient is None:
            gradient = self.gradient

        size = values.get('batch_size')
        if size is None:
            size = self.size

        interval = values.get('batch_internval')
        if interval is None:
            interval = self.interval

        self.gradient = gradient
        self.size = size
        self.interval = interval
