import time
import asyncio
import math
from async_tools.datatypes.async_list import AsyncList
from hedra.core.personas.types.default_persona import DefaultPersona
from async_tools.functions import awaitable
from .user_sequence_persona import UserSequencePersona


class MultiUserSequencePersona(DefaultPersona):

    def __init__(self, config, handler):
        super(MultiUserSequencePersona, self).__init__(
            config,
            handler
        )
        self.users = AsyncList()
        self._cli_config = config

    @classmethod
    def about(cls):
        return '''
        Multi-User-Sequence Persona - (multi-user-sequence)

        Executes multiple collections of ordered sequences where each action in a sequence represents a "step". Each step is executed as a batch, allowing for 
        easy and logical maximization of concurrency. The batch size for each step can be set either using optimization or via the --batch-size argument, and 
        the persona will attempt to complete as many actions as possible for a given step over the specified amount of time-per-batch (specified either via optimization 
        or the --batch-time argument). As with other personas, the Multi-User-Sequence persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, actions):

        user_names = await actions.parser.users()
        users_count = await user_names.size()
        actions = await actions.parser.sort()

        async for user_name in user_names:

            user_sequence_persona = UserSequencePersona(
                config=self._cli_config,
                handler=self.handler
            )

            user_actions = actions[user_name]
            user_sequence_persona.batch.size = await awaitable(
                math.ceil,
                self.batch.size/users_count
            )
            
            await user_sequence_persona.setup(user_actions)
            await self.users.append(user_sequence_persona)

    async def load_batches(self):
        async for user_sequence in self.users:
            await user_sequence.load_batches()

    async def execute(self):
        elapsed = 0
        results = []

        await self.start_updates()

        self.start = time.time()

        completed_users, _ = await asyncio.wait([
            user.execute() async for user in self.users
        ])

        elapsed = time.time() - self.start
        self.batch.deferred += [completed_users]

        self.end = time.time()

        await self.stop_updates()

        self.total_elapsed = self.end - self.start

        completed_sequences = []
        for deferred_users in self.batch.deferred:
            completed_users = await asyncio.gather(*deferred_users)
            for completed_sequence in completed_users:
                completed_sequences += [completed_sequence]

        for completed_sequence in completed_sequences:
            results += completed_sequence

        self.total_actions = len(results)
        self.total_elapsed = elapsed
                        
        return results

    async def close(self):
        for multisequence in self.users:
            await multisequence.close()

    async def get_completed_actions(self):
        total_actions = 0

        for user in self.users:
            total_actions += await user.get_completed_actions()

        return total_actions