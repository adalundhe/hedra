import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from hedra.core.parsing.actions_parser import ActionsParser
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine
from hedra.core.personas.batching import SequenceStep
from hedra.test.hooks.types import HookType
from hedra.test.stages.execute import Execute


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

        self.elapsed = 0
        self.no_execution_actions = True

    async def setup(self, sequence: Execute):

        self.session_logger.debug('Setting up persona...')

        self.engine.teardown_actions = sequence.hooks.get(HookType.TEARDOWN, [])

        actions = sequence.hooks[HookType.ACTION]

        self.actions = actions
        self.actions_count = len(actions)
        self._hooks = actions

        for setup_action in sequence.hooks.get(HookType.SETUP):
            await setup_action.call(sequence)

        self.engine.teardown_actions = sequence.hooks.get(HookType.TEARDOWN)