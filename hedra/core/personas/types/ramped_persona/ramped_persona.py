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

        elapsed = 0
        results = []

        await self.start_updates()
        current_action_idx = 0

        self.start = time.time()

        batch_size = math.ceil(self.batch.size * self.batch.gradient)

        while elapsed < self.total_time:
            next_timeout = self.total_time - elapsed
            action = self._parsed_actions[current_action_idx]

            if action.before_batch:
                action = await action.before_batch(action)
            
            self.batch.deferred.append(asyncio.create_task(
                action.session.batch_request(
                    action.parsed,
                    concurrency=batch_size,
                    timeout=next_timeout
                )
            ))
            

            await asyncio.sleep(self.batch.interval.period)

            if action.after_batch:
                action = await action.after_batch(action)

            elapsed = time.time() - self.start

            batch_size = math.ceil(batch_size * (1 + self.batch.gradient))
            current_action_idx = (current_action_idx + 1) % self.actions_count
            
        self.end = elapsed + self.start

        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            batch, pending = await deferred_batch
            completed = await asyncio.gather(*batch)
            results.extend(completed)

            try:
                for pend in pending:
                    pend.cancel()
            except Exception:
                pass

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results
