from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.engines.client.config import Config


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

    def __init__(self, config: Config):
        super(SequencedPersonaCollection, self).__init__(config)

        self.elapsed = 0
        self.no_execution_actions = True

    async def setup(self, sequence):

        self.session_logger.debug('Setting up persona...')

        actions = sequence.hooks[HookType.ACTION]

        self.actions = actions
        self.actions_count = len(actions)
        self._hooks = actions

        for setup_action in sequence.hooks.get(HookType.SETUP):
            await setup_action.call(sequence)

        self.engine.teardown_actions = sequence.hooks.get(HookType.TEARDOWN)