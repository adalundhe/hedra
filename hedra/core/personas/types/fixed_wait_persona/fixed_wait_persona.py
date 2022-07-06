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

        while elapsed < self.total_time:
            action = self._parsed_actions[current_action_idx]

            self.batch.deferred.append(asyncio.create_task(
                action.session.execute_batch(
                    action.parsed,
                    concurrency=self.batch.size,
                    timeout=self.batch.interval.period
                )
            ))

            await asyncio.sleep(self.batch.interval.period)

            elapsed = time.time() - self.start

            current_action_idx = (current_action_idx + 1) % self.actions_count

        self.end = elapsed + self.start
        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            batch, pending = await deferred_batch
            collected = await asyncio.gather(*batch)
            results.extend(collected)
            
            try:
                for pend in pending:
                    pend.cancel()
            except Exception as e:
                pass
            
            
        self.total_actions = len(results)
        self.total_elapsed = elapsed
        self.optimized_params = None

        return results
