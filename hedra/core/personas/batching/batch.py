import psutil
from .batch_interval import BatchInterval
from hedra.core.personas.utils import parse_time


class Batch:

    def __init__(self, config) -> None:
        self.config = config
        total_time = parse_time(
            self.config.get('total_time', 60)
        )
        self.gradient = self.config.get('gradient', 0.1)
        self.time = self.config.get('batch_time', total_time * 0.2)

        cpu_count = psutil.cpu_count(logical=False)
        default_batch_size = (cpu_count + 2)**4

        self.size = self.config.get('batch_size', default_batch_size)
        self.count = self.config.get('batch_count', 10)

        self.interval = BatchInterval(self.config)

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