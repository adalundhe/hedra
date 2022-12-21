import time
import asyncio
import uuid
import psutil
from hedra.tools.data_structures.async_list import AsyncList
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class RampedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(RampedPersona, self).__init__(config)

        self.persona_id = str(uuid.uuid4())
        self.type = PersonaTypes.RAMPED
        
    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)
        generation_batch_size = int(self.batch.size * self.batch.gradient)
        self._hooks[action_idx].session.shrink_pool(self.batch.size - generation_batch_size)

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%generation_batch_size == 0:
                increase_amount = int(self.batch.gradient  * self.batch.size)
                next_batch_size = generation_batch_size + increase_amount

                if next_batch_size < self.batch.size:
                    self._hooks[action_idx].session.extend_pool(increase_amount)
                    generation_batch_size = next_batch_size

                elif next_batch_size > self.batch.size:
                    increase_amount = self.batch.size - generation_batch_size

                    next_batch_size = generation_batch_size + increase_amount

                    self._hooks[action_idx].session.extend_pool(increase_amount)
                    generation_batch_size = next_batch_size
                
                await asyncio.sleep(self.batch.interval)
            
            action_idx = (action_idx + 1) % self.actions_count

            if self._hooks[action_idx].session.active%max_pool_size == 0:
                    try:
                        max_wait = total_time - elapsed
                        await asyncio.wait_for(
                            self._hooks[action_idx].session.wait_for_active_threshold(),
                            timeout=max_wait
                        )
                    except asyncio.TimeoutError:
                        pass
