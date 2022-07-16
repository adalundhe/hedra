import time
import asyncio
from async_tools.functions import awaitable
from hedra.core.personas.types.default_persona import DefaultPersona


class RampedIntervalPersona(DefaultPersona):

    def __init__(self, config, handler):
        super(RampedIntervalPersona, self).__init__(config, handler)
        self._current_batch = 1

    @classmethod
    def about(cls):
        return '''
        Ramped Interval Persona - (ramped-interval)

        Executes actions over increasing periods of batch time for the total time specified via the --total-time argument. 
        Initial batch time is set as the percentage specified by --batch-gradient argument (for example, 0.1 is 10% percent
        of specified batch time). For each subsequent batch, batch time in increased by the batch gradient amount 
        until the total time is up. You may also specify a wait between batches for an integere number of seconds
        via the --batch-interval argument.
        '''
            
    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        generation_batch_interval = self.batch.time

        start = time.time()
        batch_start = time.time()
        while elapsed < total_time:
            yield idx%self.actions_count
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            batch_elapsed = time.time() - batch_start
            idx += 1

            if batch_elapsed >= generation_batch_interval:
                generation_batch_interval = generation_batch_interval *(self.batch.gradient + 1)
                await asyncio.sleep(self.batch.interval.period)
                batch_start = time.time()