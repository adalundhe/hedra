import asyncio
import time
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config


class CyclicNoWaitPersona(DefaultPersona):

    def __init__(self, config: Config):
        super().__init__(config)

    async def generator(self, total_time):
        elapsed = 0
        idx = 0

        start = time.time()
        while elapsed < total_time:
            yield idx%self.actions_count
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1