import asyncio
from hedra.execution.personas.utils import time
import random
from zebra_async_tools.functions import awaitable


class BatchInterval:
    
    def __init__(self, config) -> None:
        self.period = config.get('batch_interval', 1)
        interval_range = config.get('batch_interval_range')
        self.period_min = None
        self.period_max = None
        
        if self.period == 0:
            self._wait_function = self._noop_wait
            self.interval_type = 'no-op'
        elif interval_range:
            self._wait_function = self._wait_random_interval
            interval_range = interval_range.split(':')
            self.period_min = int(interval_range[0])
            self.period_max = int(interval_range[1])
            self.interval_type = 'range'
        else:
            self._wait_function = self._wait_static_interval
            self.interval_type = 'static'

    @classmethod
    def about(cls):
        return '''
        Batch Interval

        key arguments:

        --batch-interval <fixed_interval> (defaults to 1 second)

        --batch-interval-range <min_interval>:<max_interval> (defaults to 1:5 seconds)

        Batch intervals allow for either fixed or randomized wait times between execution of batches. Fixed intervals
        are specified via the --batch-interval argument as an (integer) number of seconds. Randomized intervals
        are specified as a colon-seperated pair of (integer) seconds via the --batch-interval-range argument. For
        further information on how each persona implements batch intervals, view the documentation for that persona via
        the command:

            hedra --about personas:<persona_type>


        Note that excessive waits between batches will greatly reduce Hedra's throughput.


        Related Topics:

        - batches
        - personas

        '''

    def __mul__(self, other):
        if self.interval_type == 'no-op':
            return 0

        elif self.interval_type == 'range':
            return self.period_max * other

        else:
            return self.period * other

    def __rmul__(self, other):
        return self.__mul__(other)

    async def wait(self):
        return await self._wait_function()

    async def _noop_wait(self):
        pass

    async def _wait_static_interval(self):
        return await asyncio.sleep(self.period)

    async def _wait_random_interval(self):
        self.period = await awaitable(
            random.randint,
            self.period_min,
            self.period_max
        )
        return await awaitable(time.sleep, self.period)