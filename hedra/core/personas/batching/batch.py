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

    @classmethod
    def about(cls):
        return '''
        
        Batch

        key arguments:

        --batch-size <number_of_actions_per_batch> (defaults to 1000)

        --batch-time <time_limit_per_batch> (defaults to 10 seconds)

        --batch-count <number_of_batches> (defaults to 10)

        --batch-gradient <decimal_percentage> (defaults to 0.1 or 10 percent)

        Batches represent logical groupings of actions, which Hedra's personas use to maximize concurrency during execution. As
        opposed to more abstract concepts like "VUs" (virtual users), batches allow you to easily define how many concurrent 
        connections Hedra makes at once, the amount of time Hedra will wait for concurrent actions to complete, how ramping
        will affect concurrency or completion of concurrent actions, etc.
        
        Batches also allow for easy implementation of waits between batches via batch intervals, which you can learn about 
        more by running the command:

        hedra --about personas:batches:intervals

        Note both batch size and batch time *heavily* affect how many actions Hedra executes per second. To high a batch
        size will result in requests timing out due to overwhelming the target, where too small a batch size will "starve"
        Hedra's ability to maximize concurrency. A large batch time may allow for larger batch sizes but prevent Hedra from 
        iterating quickly (this particularly affects sequence-based personas), where too small a batch time will result
        in failure to complete batches and thus low thoughtput.


        Related Topics:

        - personas
        - optimizers
        - engines

        '''