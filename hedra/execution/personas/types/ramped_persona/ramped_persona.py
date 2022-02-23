import time
import asyncio
import math
from async_tools.functions import awaitable
from async_tools.datatypes.async_list import AsyncList
from hedra.execution.engines import Engine
from hedra.execution.personas.types.default_persona import DefaultPersona


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
        await self.engine.setup(self._initial_actions)


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
            deferred_actions, _ = await asyncio.wait([
                request async for request in self.engine.defer_all(self.actions)
            ], timeout=self.batch.time)

            elapsed = time.time() - self.start

            await self.batch.interval.wait()

            self.batch.deferred += [deferred_actions]
            self.actions = await self.next_batch()

        self.end = time.time()

        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            results += await asyncio.gather(*deferred_batch)

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results