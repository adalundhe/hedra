import time
import asyncio
from async_tools.functions import awaitable
from hedra.core.personas.types.default_persona import DefaultPersona


class RampedIntervalPersona(DefaultPersona):

    def __init__(self, config, handler):
        super(RampedIntervalPersona, self).__init__(config, handler)
        self._current_batch = 1

    @classmethod
    def about(cls):
        return '''
        Ramped Interval Persona - (ramped-interval)

        Executes actions over increasing periods of batch time for the total time specified via the --total-time argument. 
        Initial batch time is set as the percentage specified by --batch-gradient argument (for example, 0.1 is 10% percent
        of specified batch time). For each subsequent batch, batch time in increased by the batch gradient amount 
        until the total time is up. You may also specify a wait between batches for an integere number of seconds
        via the --batch-interval argument.
        '''
            
    async def execute(self):
        results = []
        elapsed = 0

        await self.start_updates()
        current_action_idx = 0

        self.start = time.time()

        batch_timeout = self.batch.time
        
        while elapsed < self.total_time: 

            action = self._parsed_actions[current_action_idx]
            
            self.batch.deferred.append(asyncio.create_task(
                action.session.execute_batch(
                    action.parsed,
                    concurrency=self.batch.size,
                    timeout=batch_timeout
                )
            ))

            await asyncio.sleep(self.batch.interval.period)

            elapsed = time.time() - self.start

            batch_timeout = batch_timeout + (1 * self.batch.gradient)
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

    async def _execute_batch(self):  
        return await asyncio.wait(
            [ action async for action in self.engine.defer_all(self.actions)], 
            timeout=self.batch.time
        )

    async def _next_batch_time_limit(self):
        return await awaitable(
            round,
            (self.batch.gradient + 1) * self.batch.time,
            2
        )
