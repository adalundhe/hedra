import math
from re import S
import time
import asyncio
from hedra.execution.personas.types.default_persona import DefaultPersona
from .sequence_persona import SequencedPersonaCollection
from async_tools.datatypes import AsyncList
from async_tools.functions import awaitable
from hedra.execution.engines import Engine


class MultiSequencePersona(DefaultPersona):

    def __init__(self, config=None, handler=None, user=None):
        super(MultiSequencePersona, self).__init__(config, handler)
        self._sequence_config = config
        self.sequences = AsyncList()

    @classmethod
    def about(cls):
        return '''
        Multi-Sequence Persona - (multi-sequence)

        Executes multiple ordered sequences of action where each action represents a "step". Each step is executed as a batch, allowing for easy and logical
        maximization of concurrency. The batch size for each step can be set either using optimization or via the --batch-size argument, and the persona
        will attempt to complete as many actions as possible for a given step over the specified amount of time-per-batch (specified either via optimization 
        or the --batch-time argument). As with other personas, the Multi-Sequence persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, actions):
        self.actions = actions
        self.session_logger.debug('Setting up persona...')

    async def load_batches(self):
        sequences = await self.actions.parser.sort()
        sequences_count = await self.actions.parser.count()


        for sequence_name in sequences:
            
            sequence = SequencedPersonaCollection(
                self._sequence_config,
                self.handler
            )

            sequence_actions = sequences[sequence_name]
            sequence.batch.size = await awaitable(
                math.ceil,
                self.batch.size/sequences_count
            )
            
            await sequence.setup(sequence_actions)
            await sequence.load_batches()
            await self.sequences.append(sequence)
            
        
        self.duration = self.total_time

    async def execute(self):
        results = []

        await self.start_updates()

        self.start = time.time()

        completed_sequences, _ = await asyncio.wait([
            sequence.execute() async for sequence in self.sequences
        ])

        self.batch.deferred += [completed_sequences]

        await self.batch.interval.wait()

        self.end = time.time()

        await self.stop_updates()

        self.total_elapsed = self.end - self.start

        for deferred_sequence_step in self.batch.deferred:
            results_batch = await asyncio.gather(*deferred_sequence_step)
            for batch in results_batch:
                results += batch

        self.total_actions = len(results)

        return results

    async def get_completed_actions(self):
        total_actions = 0
        for sequence in self.sequences:
            total_actions += await sequence.get_completed_actions()

        return total_actions

            


        
            
