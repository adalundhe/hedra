import math
from re import S
import time
import asyncio
from typing import List
from hedra.core.personas.types.default_persona import DefaultPersona
from .sequence_persona import SequencedPersonaCollection
from async_tools.datatypes import AsyncList
from hedra.core.parsing import ActionsParser


class MultiSequencePersona(DefaultPersona):

    def __init__(self, config=None, handler=None, user=None):
        super(MultiSequencePersona, self).__init__(config, handler)
        self._sequence_config = config
        self.sequences: List[SequencedPersonaCollection] = []
        self._utility_sequences: List[SequencedPersonaCollection] = []

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

    async def setup(self, parser: ActionsParser):

        self.session_logger.debug('Setting up persona...')

        parser.sort_multisequence()

        for sequence_class in parser.action_sets.values():
       
            sequence = SequencedPersonaCollection(
                self._sequence_config, 
                self.handler
            )
            
            self.sequences.append(sequence)

        await asyncio.gather(*[sequence.setup(sequence_class) for sequence in self.sequences])

        sequences_count = len(self.sequences)
        batch_sizes = [int(self.batch.size/sequences_count) for _ in range(sequences_count)]
        batch_sizes[-1] += self.batch.size%sequences_count

        for sequence_batch_size, sequence in zip(batch_sizes,self.sequences):
            sequence.batch.size = sequence_batch_size

        
        self.sequences = AsyncList(self.sequences)
        self._utility_sequences = AsyncList(self._utility_sequences)

    async def execute(self):
        results = []

        await self.start_updates()

        sequences = []
        async for sequence in self.sequences:
            sequences.append(
                asyncio.create_task(sequence.execute())
            )

        await self.stop_updates()

        completed = await asyncio.gather(*sequences, return_exceptions=True)
        
        for complete in completed:
            results.extend(complete)

        elapsed_times = []

        async for sequence in self.sequences:
            elapsed_times.append(sequence.total_elapsed)

        self.total_elapsed = sum(elapsed_times)/len(elapsed_times)
        self.total_actions = len(results)

        return results

    async def get_completed_actions(self):
        total_actions = 0
        for sequence in self.sequences:
            total_actions += await sequence.get_completed_actions()

        return total_actions

    async def close(self):
        async for sequence in self.sequences:
            await sequence.close()

        async for sequence in self._utility_sequences:
            await sequence.close()
