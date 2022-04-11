import math
from async_tools.datatypes import AsyncList
from hedra.core.personas.types.multi_sequence_persona import MultiSequencePersona
from hedra.core.personas.types.multi_sequence_persona.sequence_persona import SequencedPersonaCollection
from async_tools.functions import awaitable


class UserSequencePersona(MultiSequencePersona):

    def __init__(self, config=None, handler=None):
        super(UserSequencePersona, self).__init__(config, handler)
        self.user_setup_actions = AsyncList()
        self.user_teardown_actions = []

    async def load_batches(self):
        sequences_count = await awaitable(
            len,
            self.actions
        )

        for sequence_idx, sequence_name in enumerate(self.actions):
            
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

            if sequence_idx == 0:
                await sequence.engine.setup(
                    AsyncList(self.user_setup_actions)
                )
                
                await sequence.engine.set_teardown_actions(
                    self.user_teardown_actions
                )

            else:
                await sequence.engine.setup(AsyncList())

            await sequence.load_batches()
            await self.sequences.append(sequence)
            
        
        self.duration = self.total_time
