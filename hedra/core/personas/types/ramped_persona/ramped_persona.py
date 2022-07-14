import time
import asyncio
import math
from async_tools.functions import awaitable
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.types.default_persona import DefaultPersona


class RampedPersona(DefaultPersona):

    def __init__(self, config, handler):
        super(RampedPersona, self).__init__(config, handler)
        self._initial_actions = AsyncList()
        self._current_batch = 1

    @classmethod
    def about(cls):
        return '''
        Ramped Persona - (ramped)

        Executes actions in increasing batch sizes over the time specified via the --total-time argument. Initial 
        batch size is set as the percentage specified by --batch-gradient argument (for example, 0.1 is 10% percent
        of specified batch size). For each subsequent batch, batch size in increased by the batch gradient amount 
        until the total time is up. You may also specify a wait between batches for an integere number of seconds
        via the --batch-interval argument.
        '''

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
            ) async for idx in self.generator(total_time, self.actions_count, self.batch.size)
        ], timeout=1)
        self.end = time.time()
        self.start = start
        for pend in pending:
            try:
                pend.cancel()
            except Exception:
                pass

        completed = await asyncio.gather(*completed)
            
        self.total_actions = len(set(completed))
        self.total_elapsed = self.end - self.start
        self.optimized_params = None

        return completed
            
    async def generator(self, total_time, actions_count, batch_size):
        elapsed = 0
        idx = 0
        action_idx = 0
        generation_batch_size = batch_size

        start = time.time()
        while elapsed < total_time:
            yield idx, action_idx
            
            await asyncio.sleep(0)
            elapsed = time.time() - start
            idx += 1

            if idx >= generation_batch_size:
                generation_batch_size = generation_batch_size *(self.batch.gradient + 1)
                action_idx = (action_idx + 1) % actions_count
                await asyncio.sleep(self.batch.interval.period)
