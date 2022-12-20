import random
import time
import asyncio
import uuid
import psutil
from typing import Dict, List, Union
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.client.config import Config
from hedra.core.personas.types.types import PersonaTypes


class WeightedSelectionPersona(DefaultPersona):
    
    __slots__ = (
        'weights',
        'indexes',
        'sample'
    )

    def __init__(self, config: Config):
        super().__init__(config)

        self.persona_id = str(uuid.uuid4())
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

    def setup(self, hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]], metadata_string: str):

        self.metadata_string = f'{metadata_string} Persona: {self.type.capitalize()}:{self.persona_id} - '
        
        actions = hooks.get(HookType.ACTION)
        actions.extend(
            hooks.get(HookType.TASK)
        )
        self.actions_count = len(actions)

        self.weights = [action.metadata.weight for action in actions]
        self.indexes = [idx for idx in range(self.actions_count)]

        self.sample = random.choices(
            self.indexes,
            weights=self.weights,
            k=self.batch.size
        )

        self._hooks = actions


    async def generator(self, total_time):
        elapsed = 0
        max_pool_size = int(self.batch.size * (psutil.cpu_count(logical=False) * 2)/self.workers)

        start = time.time()
        while elapsed < total_time:
            for action_idx in self.sample:
                yield action_idx
                
                await asyncio.sleep(0)
                elapsed = time.time() - start
            
                if self._hooks[action_idx].session.active%max_pool_size == 0:
                    try:
                        max_wait = total_time - elapsed
                        await asyncio.wait_for(
                            self._hooks[action_idx].session.wait_for_active_threshold(),
                            timeout=max_wait
                        )
                    except asyncio.TimeoutError:
                        pass

            self.sample = random.choices(
                self.indexes,
                weights=self.weights,
                k=self.batch.size
            )
