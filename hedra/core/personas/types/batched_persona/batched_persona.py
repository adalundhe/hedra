import asyncio
import time
from hedra.core.personas.types.default_persona import DefaultPersona


class BatchedPersona(DefaultPersona):

    def __init__(self, config, handler):
        super().__init__(config, handler)

    async def generator(self, total_time, actions_count, batch_size):
        elapsed = 0
        idx = 0
        action_idx = 0

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%batch_size == 0:
                action_idx = (action_idx + 1)%actions_count
                await asyncio.sleep(self.batch.interval.period)
