import random
import time
import asyncio
from typing import List
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.hooks.types.types import HookType
from hedra.core.hooks.client.config import Config


class WeightedSelectionPersona(DefaultPersona):

    def __init__(self, config: Config):
        super().__init__(config)

        self.weights: List[int] = []
        self.indexes: List[int] = []
        self.sample: List[int] = []
        
    @classmethod
    def about(cls):
        return '''
        Weighted Persona - (sequence)

        Executes batches of actions of the batch size specified by the --batch-size  argument. Actions for each batch are resampled each iteration according 
        to their specified "weight". As with other personas, the Weighted persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, actions):
        self.session_logger.debug('Setting up persona...')
        
        actions.weights()

        self.indexes = []
        self.actions = []
        self.weights = []

        for idx, action, weight in actions:
            self.indexes.append(idx)
            self.actions.append(action)
            self.weights.append(weight)

        self.sample = random.choices(
            self.indexes,
            weights=self.weights,
            k=self.batch.size
        )

        self.actions_count = len(self.actions)
        self._hooks = self.actions

        self.engine.teardown_actions = []

        for action_set in actions:

            setup_hooks = action_set.hooks.get(HookType.SETUP)

            for setup_hook in setup_hooks:
                await setup_hook.call(action_set)

            teardown_hooks = action_set.hooks.get(HookType.TEARDOWN)
            if teardown_hooks:
                self.engine.teardown_actions.extend(teardown_hooks)

    async def generator(self, total_time):
        elapsed = 0

        start = time.time()
        while elapsed < total_time:
            for action_idx in self.sample:
                yield action_idx
                
                await asyncio.sleep(0)
                elapsed = time.time() - start

            self.sample = random.choices(
                self.indexes,
                weights=self.weights,
                k=self.batch.size
            )
            
