import time
import asyncio
import math
from async_tools.functions import awaitable
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config


class RampedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(RampedPersona, self).__init__(config)
        self._initial_actions = AsyncList()
        self._current_batch = 1

    @classmethod
    def about(cls):
        return '''
        Ramped Persona - (ramped)

        Executes actions in increasing batch sizes over the time specified via the --total-time argument. Initial 
        batch size is set as the percentage specified by --batch-gradient argument (for example, 0.1 is 10% percent
        of specified batch size). For each subsequent batch, batch size in increased by the batch gradient amount 
        until the total time is up. You may also specify a wait between batches for an integere number of seconds
        via the --batch-interval argument.
        '''
            
    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
        generation_batch_size = self.batch.size

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx >= generation_batch_size:
                generation_batch_size = generation_batch_size * (self.batch.gradient + 1)
                action_idx = (action_idx + 1) % self.actions_count
                await asyncio.sleep(self.batch.interval.period)
