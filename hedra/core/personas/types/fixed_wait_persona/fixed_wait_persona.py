import asyncio
import time
from hedra.core.personas.types.default_persona import DefaultPersona


class FixedWaitPersona(DefaultPersona):

    def __init__(self, config, handler):
        super().__init__(config, handler)

    async def execute(self):

        elapsed = 0
        results = []

        await self.start_updates()
        
        self.start = time.time()

        while elapsed < self.duration:
            batch = await asyncio.wait(
                [ request async for request in self.engine.defer_all(self.actions)], 
                timeout=self.batch.time
            )
            self.batch.deferred.append(batch)
            elapsed = time.time() - self.start

        self.end = elapsed + self.start
        await self.stop_updates()

        for deferred_batch, pending in self.batch.deferred:
            batch = await asyncio.gather(*deferred_batch, return_exceptions=True)
            results.extend(batch)
            
            try:
                await asyncio.gather(*pending)
            except Exception:
                pass
            
        self.total_actions = len(results)
        self.total_elapsed = elapsed
        self.optimized_params = None

        return results

