import time
import asyncio
from async_tools.datatypes.async_list import AsyncList
from yaml import parse
from hedra.core.personas.batching.batch_interval import BatchInterval
from hedra.core.personas.types.default_persona import DefaultPersona
from hedra.core.engines import Engine
from hedra.core.personas.batching import SequenceStep
from hedra.core.parsing import ActionsParser
from hedra.test.hooks.hook import Hook
from hedra.test.hooks.types import HookType


class SequencedPersonaCollection(DefaultPersona):

    def __init__(self, config, handler):
        super(SequencedPersonaCollection, self).__init__(
            config,
            handler
        )

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

    async def setup(self, parser: ActionsParser):

        self.session_logger.debug('Setting up persona...')

        parser.sort_sequence()

        self.actions = parser.actions
        self.actions_count = len(self.actions)
        self._hooks = self.actions

        self.engine.teardown_actions = []

        for action_set in parser.action_sets.values():

            setup_hooks = action_set.hooks.get(HookType.SETUP)

            for setup_hook in setup_hooks:
                await setup_hook.call(action_set)

            teardown_hooks = action_set.hooks.get(HookType.TEARDOWN)
            if teardown_hooks:
                self.engine.teardown_actions.extend(teardown_hooks)
