import time
import asyncio
from typing import AsyncIterable, Dict, List
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.plugins.types.persona import (
    PersonaPlugin,
    setup,
    generate,
    shutdown
)


class CustomPersona(PersonaPlugin):

    @setup()
    async def setup(self, hooks: Dict[HookType, List[Hook]]):
        return super().setup(hooks)

    @generate()
    async def generate_next(self) -> AsyncIterable[int]:
        total_time = self.total_time
        elapsed = 0
        idx = 0

        start = time.time()

        while elapsed < total_time:
            yield idx%self.actions_count
            await asyncio.sleep(0)
            
            idx += 1
            

        elapsed = time.time() - start

    @shutdown()
    async def shutdown(self):
        pass