import time
import asyncio
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class RampedIntervalPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(RampedIntervalPersona, self).__init__(config)
        self.type = PersonaTypes.RAMPED_INTERVAL
            
    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        generation_batch_interval = self.batch.interval * self.batch.gradient

        start = time.time()
        batch_start = time.time()
        while elapsed < total_time:
            yield idx%self.actions_count
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            batch_elapsed = time.time() - batch_start
            idx += 1

            if batch_elapsed >= generation_batch_interval:
                increase_amount = (self.batch.interval * self.batch.gradient)
                next_batch_time = generation_batch_interval + increase_amount

                if next_batch_time < self.batch.interval:
                    generation_batch_interval = next_batch_time
                else:
                    generation_batch_interval = self.batch.interval
                
                await asyncio.sleep(self.batch.interval)
                batch_start = time.time()
