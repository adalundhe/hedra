import random
import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from async_tools.functions import awaitable
from hedra.core.engines import Engine
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.parsing import ActionsParser


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

    async def setup(self, parser: ActionsParser):

        self.session_logger.debug('Setting up persona...')

        self.actions = parser.actions
        self.actions_count = len(self.actions)
        self.weights = await parser.weights()

        await self.engine.create_session(parser.setup_actions)  
        self.engine.teardown_actions = parser.teardown_actions

        for action in self.actions:
            parsed_action =  await action()
            self._parsed_actions.data.append(parsed_action)

        self.sampled_actions = self._sample()
        self.duration = self.total_time

    async def execute(self):
        elapsed = 0
        results = []

        await self.start_updates()

        self.start = time.time()

        while elapsed < self.total_time:
            next_timeout = self.total_time - (time.time() - self.start)

            action = self.sampled_actions.pop()
            next_timeout = self.total_time - elapsed
            
            self.batch.deferred.append(asyncio.create_task(
                action.session.batch_request(
                    action.data,
                    concurrency=self.batch.size,
                    timeout=next_timeout
                )
            ))
            
            await asyncio.sleep(self.batch.interval.period)

            elapsed = time.time() - self.start

            if len(self.sampled_actions) < 1:
                self.sampled_actions = self._sample()

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

    def _sample(self):
        return random.choices(
            self._parsed_actions.data,
            self.weights,
            k=self.actions_count
        )
