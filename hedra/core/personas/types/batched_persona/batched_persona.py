import asyncio
import time
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class BatchedPersona(DefaultPersona):

    def __init__(self, config: Config):
        super().__init__(config)
        self.type = PersonaTypes.BATCHED

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx%self.batch.size == 0:
                action_idx = (action_idx + 1)%self.actions_count
                await asyncio.sleep(self.batch.interval.period)

        self.start = start
