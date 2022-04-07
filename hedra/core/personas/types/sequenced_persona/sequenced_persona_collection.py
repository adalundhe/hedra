import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine
from hedra.core.personas.batching import SequenceStep


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
                sequence_step = SequenceStep()
                for _ in range(self.batch.size):
                    await sequence_step.add_action(action)

                if action.wait_interval:
                    sequence_step.wait_interval = BatchInterval({
                        'batch_interval': action.wait_interval
                    })

                else:
                    if self.batch.interval.interval_type == 'no-op':
                        sequence_step.wait_interval = self.batch.time

                    else:
                        sequence_step.wait_interval = self.batch.interval

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
                self.batch.deferred.append(
                    asyncio.create_task(
                        self._execute_batch(sequence_step)
                    )
                )

                await sequence_step.wait_interval.wait()
                elapsed = time.time() - self.start

        self.end = elapsed + self.start

        await self.stop_updates()

        for deferred_batch in self.batch.deferred:
            completed, pending = await deferred_batch
            completed = await asyncio.gather(*completed)
            results.extend(completed)
            
            try:
                await asyncio.gather(*pending)
            except Exception:
                pass

        self.total_actions = len(results)
        self.total_elapsed = elapsed

        return results

    async def _execute_batch(self, sequence_step: SequenceStep):
        next_timeout = self.duration - (time.time() - self.start)  
        return await asyncio.wait([
            action async for action in self.engine.defer_all(sequence_step.actions)
        ], timeout=next_timeout)