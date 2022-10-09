import random
import time
import asyncio
from typing import Dict, List
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class WeightedSelectionPersona(DefaultPersona):

    def __init__(self, config: Config):
        super().__init__(config)

        self.weights: List[int] = []
        self.indexes: List[int] = []
        self.sample: List[int] = []
        self.type = PersonaTypes.WEIGHTED
        
    @classmethod
    def about(cls):
        return '''
        Weighted Persona - (sequence)

        Executes batches of actions of the batch size specified by the --batch-size  argument. Actions for each batch are resampled each iteration according 
        to their specified "weight". As with other personas, the Weighted persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, hooks: Dict[HookType, List[Hook]]):
        self.session_logger.debug('Setting up persona...')
        
        actions = hooks.get(HookType.ACTION)
        self.actions_count = len(actions)

        self.weights = [action.config.weight for action in actions]
        self.indexes = [idx for idx in range(self.actions_count)]

        self.sample = random.choices(
            self.indexes,
            weights=self.weights,
            k=self.batch.size
        )

        self.actions_count = len(self.actions)
        self._hooks = actions


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
