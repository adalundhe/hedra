import time
import asyncio
from hedra.execution.personas.types.default_persona import DefaultPersona
from hedra.execution.personas.batching import Batch


class IntervalPersona(DefaultPersona):

    def __init__(self, config, handler):
        super(IntervalPersona, self).__init__(config, handler)

    @classmethod
    def about(cls):
        return '''
        Interval - (interval)

        Executes as many actions as possible of the specified batch size (can be set either using optimization or via the --batch-size argument) for the
        specified amount of time-per-batch (specified either via optimization or the --batch-time argument) for the total amount of time specified by 
        the --total-time argument. Unlike the Default persona, the Interval persona will pause execution between batches for the amount of time specified
        by the --batch-interval argument.
        '''
        
    async def execute(self):

        elapsed = 0
        results = []

        await self.start_updates()

        self.start = time.time()

        while elapsed < self.duration:
            deferred_actions, _ = await asyncio.wait([
                request async for request in self.engine.defer_all(self.actions)
            ], timeout=self.batch.time)

            elapsed = time.time() - self.start

            await self.batch.interval.wait()
            self.batch.deferred += [deferred_actions]

        self.end = time.time()

        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            results += await asyncio.gather(*deferred_batch)

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results
