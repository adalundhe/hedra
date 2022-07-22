from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.hooks.types.types import HookType
from hedra.core.hooks.client.config import Config


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

    async def setup(self, actions):

        self.session_logger.debug('Setting up persona...')


        self.actions = actions
        self.actions_count = len(self.actions)
        self._hooks = self.actions

        self.engine.teardown_actions = []

        for action_set in actions:

            setup_hooks = action_set.hooks.get(HookType.SETUP)

            for setup_hook in setup_hooks:
                await setup_hook.call()

            teardown_hooks = action_set.hooks.get(HookType.TEARDOWN)
            if teardown_hooks:
                self.engine.teardown_actions.extend(teardown_hooks)
