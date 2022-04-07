import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine
from hedra.core.personas.batching import SequenceStep


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

        for deferred_batch in self.batch.deferred:
            completed, pending = await deferred_batch
            completed = await asyncio.gather(*completed)
            results.extend(completed)
            
            try:
                await asyncio.gather(*pending)
            except Exception:
                pass

            try:
                await asyncio.gather(*pending)
            except Exception:
                pass

        return results

    async def _execute_batch(self, sequence_step: SequenceStep):
        next_timeout = self.duration - (time.time() - self.start)   
        return await asyncio.wait([
            action async for action in self.engine.defer_all(sequence_step.actions)
        ], timeout=next_timeout)
        
