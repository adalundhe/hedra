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

        current_action_idx = 0

        while elapsed < self.duration:
            batch = await self._parsed_actions[current_action_idx].execute(self.batch.time)
            self.batch.deferred.append(batch)
            elapsed = time.time() - self.start

            current_action_idx = (current_action_idx + 1) % self.actions_count

        self.end = elapsed + self.start
        await self.stop_updates()

        for deferred_batch, pending in self.batch.deferred:
            batch = await asyncio.gather(*deferred_batch, return_exceptions=True)
            results.extend(batch)
            
            try:
                for pend in pending:
                    pend.cancel()
            except Exception:
                pass
            
        self.total_actions = len(results)
        self.total_elapsed = elapsed
        self.optimized_params = None

        return results
