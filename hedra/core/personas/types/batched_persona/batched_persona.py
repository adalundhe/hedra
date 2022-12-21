import asyncio
import time
import uuid
import psutil
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class BatchedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super().__init__(config)

        self.persona_id = str(uuid.uuid4())

        self.type = PersonaTypes.BATCHED

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%self.batch.size == 0:
                action_idx = (action_idx + 1)%self.actions_count
                await asyncio.sleep(self.batch.interval)

            if self._hooks[action_idx].session.active%max_pool_size == 0:
                    try:
                        max_wait = total_time - elapsed
                        await asyncio.wait_for(
                            self._hooks[action_idx].session.wait_for_active_threshold(),
                            timeout=max_wait
                        )
                    except asyncio.TimeoutError:
                        pass


        self.start = start
