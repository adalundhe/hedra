import math
from hedra.execution.personas.types.multi_sequence_persona import MultiSequencePersona
from hedra.execution.personas.types.multi_sequence_persona.sequence_persona import SequencedPersonaCollection
from async_tools.functions import awaitable


class UserSequencePersona(MultiSequencePersona):

    def __init__(self, config=None, handler=None):
        super(UserSequencePersona, self).__init__(config, handler)

    async def load_batches(self):
        sequences_count = await awaitable(
            len,
            self.actions
        )

        for sequence_name in self.actions:
            
            sequence = SequencedPersonaCollection(
                self._sequence_config,
                self.handler
            )

            sequence_actions = self.actions[sequence_name]
            sequence.batch.size = await awaitable(
                math.ceil,
                self.batch.size/sequences_count
            )
            
            await sequence.setup(sequence_actions)
            await sequence.load_batches()
            await self.sequences.append(sequence)
            
        
        self.duration = self.total_time
    
