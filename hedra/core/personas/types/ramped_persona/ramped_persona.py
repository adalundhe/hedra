import time
import asyncio
import math
from async_tools.functions import awaitable
from async_tools.datatypes.async_list import AsyncList
from hedra.core.engines import Engine
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

    async def setup(self, actions):
        self.session_logger.debug('Setting up persona...')

        self._initial_actions = await actions.to_async_list()
        self.actions_count = await self._initial_actions.size()

        self.engine = Engine(self.config, self.handler)
        await self.engine.setup(AsyncList(actions.parser.setup_actions))
        await self.engine.set_teardown_actions(
            actions.parser.teardown_actions
        )


    async def load_batches(self):
        self.actions = await self.next_batch()
        self.duration = self.total_time

    async def next_batch(self):
        batch_size = await self._next_batch_size()

        actions = AsyncList()
        for idx in range(batch_size):
            action_idx = idx % self.actions_count
            await actions.append(self._initial_actions[action_idx])

        self._current_batch += 1

        return actions

    async def _next_batch_size(self):
        return await awaitable(
            math.ceil,
            (self._current_batch * self.batch.gradient * self.batch.size)
        )

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
            
            self.actions = await self.next_batch()

        self.end = time.time()

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
        next_timeout = self.duration - (time.time() - self.start)    
        return await asyncio.wait(
            [ action async for action in self.engine.defer_all(self.actions)], 
            timeout=next_timeout if next_timeout > 0 else 1
        )
