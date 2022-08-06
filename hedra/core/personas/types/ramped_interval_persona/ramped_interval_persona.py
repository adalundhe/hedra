import time
import asyncio
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config


class RampedIntervalPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(RampedIntervalPersona, self).__init__(config)
        self._current_batch = 1
            
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

                if elapsed < total_time/2:
                    generation_batch_interval = generation_batch_interval * (self.batch.gradient + 1)
                else:

                    generation_batch_interval = generation_batch_interval * (1 - self.batch.gradient)
                
                await asyncio.sleep(self.batch.interval.period)
                batch_start = time.time()