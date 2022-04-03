import random
import time
import asyncio
import math
from async_tools.datatypes.async_list import AsyncList
from async_tools.functions import awaitable
from hedra.core.engines import Engine
from hedra.core.personas.types.default_persona import DefaultPersona


class WeightedSelectionPersona(DefaultPersona):

    def __init__(self, config=None, handler=None):
        super().__init__(config=config, handler=handler)
        self.weights = []
        self.sampled_actions = AsyncList()
        
    @classmethod
    def about(cls):
        return '''
        Weighted Persona - (sequence)

        Executes batches of actions of the batch size specified by the --batch-size  argument. Actions for each batch are resampled each iteration according 
        to their specified "weight". As with other personas, the Weighted persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, actions):

        self.session_logger.debug('Setting up persona...')

        self.actions = await actions.to_async_list()
        self.weights = await actions.persona.weights()
        self.actions_count = await self.actions.size()

        self.engine = Engine(self.config, self.handler)

        actions_list = await self.actions.to_async_list()
        await self.engine.setup(actions_list)

        self.sampled_actions = await self._sample()
        self.duration = self.total_time

    async def execute(self):
        elapsed = 0
        results = []
        deferred_batches = []

        await self.start_updates()

        self.start = time.time()

        while elapsed < self.duration:
            deferred_actions, _ = await asyncio.wait([
                request async for request in self.engine.defer_all(self.sampled_actions)
            ], timeout=self.batch.time)

            elapsed = time.time() - self.start

            await self.batch.interval.wait()
            deferred_batches += [deferred_actions]
            self.sampled_actions = await self._sample()

        self.end = time.time()

        await self.stop_updates()

        for deferred_batch in deferred_batches:
            results += await asyncio.gather(*deferred_batch)

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results

    async def _sample(self):
        resampled_actions = await awaitable(
            random.choices,
            self.actions.data,
            self.weights,
            k=self.batch.size
        )

        resampled_actions = filter(
            lambda action: action.is_setup is False and action.is_teardown is False,
            resampled_actions
        )

        return AsyncList(resampled_actions)
