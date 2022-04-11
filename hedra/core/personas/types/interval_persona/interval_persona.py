import time
import asyncio
from hedra.core.personas.types.default_persona import DefaultPersona


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
            self.batch.deferred.append(asyncio.create_task(
                self._execute_batch()
            ))

            await self.batch.interval.wait()
            elapsed = time.time() - self.start

        self.end = elapsed + self.start

        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            batch, pending = await deferred_batch
            completed = await asyncio.gather(*batch, return_exceptions=True)
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
            [ request async for request in self.engine.defer_all(self.actions)], 
            timeout=self.batch.time
        )
