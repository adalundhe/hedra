import asyncio
import time
from hedra.core.personas.types.default_persona import DefaultPersona


class CyclicNoWaitPersona(DefaultPersona):

    def __init__(self, config, handler):
        super().__init__(config, handler)


    async def execute(self):
        actions = self._parsed_actions
        actions_count = self.actions_count
        total_time = self.total_time
        task_idx = 0

        await self.start_updates()
        
        start = time.time()

        completed, pending = await asyncio.wait([
            asyncio.create_task(
                actions[idx%actions_count].session.execute_prepared_request(
                    actions[idx%actions_count].action,
                    idx,
                    timeout=total_time,
                )
            ) async for idx in self.generator(total_time)
        ], timeout=1)
        self.end = time.time()

        results = await asyncio.gather(*completed)

        self.start = start
        for pend in pending:
            try:
                pend.cancel()
            except Exception:
                pass

            
        self.total_actions = len(set(results))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return results

    async def generator(self, total_time):
        elapsed = 0
        idx = 0

        start = time.time()
        while elapsed < total_time:
            yield idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1