import random
import time
import asyncio
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

        self._parsed_actions = await actions.to_async_list()
        self.actions_count = await self._parsed_actions.size()

        self.engine = Engine(self.config, self.handler)
        await self.engine.setup(AsyncList(actions.parser.setup_actions))
        await self.engine.set_teardown_actions(
            actions.parser.teardown_actions
        )

        self.duration = self.total_time

    async def load_batches(self):
        self.actions = AsyncList()
        for idx in range(self.batch.size):
            action_idx = idx % self.actions_count
            action = self._parsed_actions[action_idx]

            if action.is_setup is False and action.is_teardown is False:
                await self.actions.append(action)
                self.weights.append(action.weight)
        
        self.sampled_actions = await self._sample()

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
            self.sampled_actions = await self._sample()

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

    async def _execute_batch(self):
        next_timeout = self.duration - (time.time() - self.start)    
        return await asyncio.wait(
            [ action async for action in self.engine.defer_all(self.sampled_actions)], 
            timeout=next_timeout if next_timeout > 0 else 1
        )
