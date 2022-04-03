import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine


class SequencedPersonaCollection(DefaultPersona):

    def __init__(self, config, handler):
        super(SequencedPersonaCollection, self).__init__(
            config,
            handler
        )

    @classmethod
    def about(cls):
        return '''
        Sequenced Persona - (sequence)

        Executes an ordered sequence of actions where each action represents a "step". Each step is executed as a batch, allowing for easy and logical
        maximization of concurrency. The batch size for each step can be set either using optimization or via the --batch-size argument, and the persona
        will attempt to complete as many actions as possible for a given step over the specified amount of time-per-batch (specified either via optimization 
        or the --batch-time argument). As with other personas, the Sequenced persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, actions):

        self.session_logger.debug('Setting up persona...')

        self.actions = actions
        self.engine = Engine(self.config, self.handler)

        actions_list = await self.actions.to_async_list()
        await self.engine.setup(actions_list)

    async def load_batches(self):

        actions_list = await self.actions.parser.sort()
        self.actions_count = await actions_list.size()

        batched_sequences = AsyncList()
        async for action in actions_list:
            if action.is_setup is False and action.is_teardown is False:        
                sequence_step = AsyncList()
                for _ in range(self.batch.size):
                    await sequence_step.append(action)

                await batched_sequences.append(sequence_step)

        self.actions = batched_sequences
        self.duration = self.total_time

    async def execute(self):
        elapsed = 0
        results = []

        await self.start_updates()

        self.start = time.time()

        while elapsed < self.duration:

            async for sequence_step in self.actions:
                deferred_step, _ = await asyncio.wait([
                    action async for action in self.engine.defer_all(sequence_step)
                ], timeout=self.batch.time)

                elapsed = time.time() - self.start

                await self.batch.interval.wait()
                self.batch.deferred += [deferred_step]

        self.end = time.time()

        await self.stop_updates()

        for completed_set in self.batch.deferred:
            results += await asyncio.gather(*completed_set)

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results