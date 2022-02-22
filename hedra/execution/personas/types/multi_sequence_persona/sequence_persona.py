import time
import asyncio
from zebra_async_tools.datatypes.async_list import AsyncList
from hedra.execution.personas.types.default_persona import DefaultPersona
from hedra.execution.engines import Engine


class SequencedPersonaCollection(DefaultPersona):
    '''
    ---------------------------------------
    Sequenced Persona - (sequence)
    
    Executed actions in batches according to:

    - the actions in the number of batches specified by --batch-count
    - the size specified by --batch-size
    - the order specified by an action's "order" value
    
    waiting between batches for:
    
    - the number of seconds specified by --batch-interval.
    
    Actions are sorted in ascending order prior to execution. 
    When the persona has reached the end of a sequence, it will 
    "wrap" back around and resume iteration from the start of 
    the sequence.
    '''

    def __init__(self, config, handler):
        super(SequencedPersonaCollection, self).__init__(
            config,
            handler
        )

    async def setup(self, actions):
        self.actions = actions
        self.actions_count = await actions.size()

        self.engine = Engine(self.config, self.handler)
        await self.engine.setup(actions)


    async def load_batches(self):
        batched_sequences = AsyncList()
        async for action in self.actions:
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

        self.start = time.time()

        while elapsed < self.total_time:
            async for sequence_step in self.actions:
                deferred_step, _ = await asyncio.wait([
                    action async for action in self.engine.defer_all(sequence_step)
                ], timeout=self.batch.time)

                self.batch.deferred += [deferred_step]

            elapsed = time.time() - self.start

        for deferred_sequence_step in self.batch.deferred:
            results += await asyncio.gather(*deferred_sequence_step)

        return results
        

