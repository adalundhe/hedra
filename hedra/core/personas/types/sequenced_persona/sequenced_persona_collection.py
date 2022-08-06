from typing import Dict, List
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.engines.client.config import Config


class SequencedPersonaCollection(DefaultPersona):

    def __init__(self, config: Config):
        super(SequencedPersonaCollection, self).__init__(config)

    @classmethod
    def about(cls):
        return '''
        Sequenced Persona - (sequence)

        Executes an ordered sequence of actions where each action represents a "step". Each step is executed as a batch, allowing for easy and logical
        maximization of concurrency. The batch size for each step can be set either using optimization or via the --batch-size argument, and the persona
        will attempt to complete as many actions as possible for a given step over the specified amount of time-per-batch (specified either via optimization 
        or the --batch-time argument). As with other personas, the Sequenced persona will execute for the total amount of time specified by the --total-time 
        argument. You may specify a wait between batches (between each step) by specifying an integer number of seconds via the --batch-interval argument.
        '''

    async def setup(self, hooks: Dict[HookType, List[Hook]]):
        
        sequence = sorted(
            hooks.get(HookType.ACTION),
            key=lambda action: action.config.order
        )

        self.actions_count = len(sequence)
        self._hooks = sequence
