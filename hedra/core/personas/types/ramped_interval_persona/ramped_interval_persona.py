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
        elapsed_all = 0
        wait_timings = []

        await self.start_updates()

        self.start = time.time()
        
        while elapsed_all < self.duration:
            for _ in range(self.batch.count):
                
                elapsed_batch = 0
                start = time.time()

                while elapsed_batch < self.batch.time:
                    deferred_actions, _ = await asyncio.wait([
                        request async for request in self.engine.defer_all(self.actions)
                    ], timeout=self.batch.time)

                    elapsed_batch = time.time() - start
                    self.batch.deferred += [deferred_actions]

                await self.batch.interval.wait()
                wait_timings += [self.batch.interval.period]
                self.batch.time = await self._next_batch_time_limit()
                
            elapsed_all = time.time() - self.start

        self.end = time.time()

        await self.stop_updates()


        for deferred_batch in self.batch.deferred:
            results += await asyncio.gather(*deferred_batch)

        self.total_actions = len(results)
        self.total_elapsed = (self.end - self.start) - sum(wait_timings)

        return results

    async def _next_batch_time_limit(self):
        return await awaitable(
            round,
            (self.batch.gradient + 1) * self.batch.time,
            2
        )