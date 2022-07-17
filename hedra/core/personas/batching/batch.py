from .batch_interval import BatchInterval
from hedra.core.hooks.client.config import Config


class Batch:

    def __init__(self, config: Config) -> None:
        self.gradient = config.batch_gradient

        self.size = config.batch_size

        self.interval = BatchInterval(config)

        self.deferred = []


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