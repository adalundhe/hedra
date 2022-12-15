import asyncio
import time
import uuid
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class CyclicNoWaitPersona(DefaultPersona):

    def __init__(self, config: Config):
        super().__init__(config)

        self.persona_id = str(uuid.uuid4())
        self.type = PersonaTypes.NO_WAIT

    async def generator(self, total_time):
        elapsed = 0
        idx = 0

        start = time.monotonic()
        while elapsed < total_time:
            yield idx%self.actions_count
            
            await asyncio.sleep(0)
            elapsed = time.monotonic() - start
            idx += 1
            