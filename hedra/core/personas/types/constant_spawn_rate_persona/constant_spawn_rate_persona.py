import uuid
import time
import asyncio
import psutil
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

        self.persona_id = str(uuid.uuid4())
        self.action_interval = config.action_interval
        self.type = PersonaTypes.CONSTANT_SPAWN

    async def generator(self, total_time):
        elapsed = 0
        idx = 0
        action_idx = 0
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)

        start = time.time()
        while elapsed < total_time:
            yield action_idx
            
            await asyncio.sleep(self.action_interval)
            elapsed = time.time() - start
            idx += 1

            if idx%self.batch.size == 0:
                await asyncio.sleep(self.batch.interval)

            action_idx = (action_idx + 1)%self.actions_count
            if self._hooks[action_idx].session.active%max_pool_size == 0:
                    try:
                        max_wait = total_time - elapsed
                        await asyncio.wait_for(
                            self._hooks[action_idx].session.wait_for_active_threshold(),
                            timeout=max_wait
                        )
                    except asyncio.TimeoutError:
                        pass
