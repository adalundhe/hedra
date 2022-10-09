import math
import time
import asyncio
from asyncio import Task
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.default_persona.default_persona import DefaultPersona
from hedra.core.personas.types.types import PersonaTypes


async def cancel_pending(pend: Task):
    try:
        pend.cancel()
        await pend
    
    except asyncio.CancelledError:
        pass


class ConstantSpawnPersona(DefaultPersona):

    def __init__(self, config: Config):
        super(ConstantSpawnPersona, self).__init__(config)
        self.action_interval = config.action_interval
        self.type = PersonaTypes.CONSTANT_SPAWN

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(self.action_interval)
            elapsed = time.time() - start
            idx += 1

            if idx%self.batch.size == 0:
                action_idx = (action_idx + 1)%self.actions_count
                await asyncio.sleep(self.batch.interval.period)
