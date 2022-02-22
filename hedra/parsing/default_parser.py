from hedra.parsing.actions import action
from .actions import Action
from zebra_automate_logging import Logger
from zebra_async_tools.datatypes import AsyncList


class DefaultParser:

    def __init__(self, config):
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self._raw_actions = config.actions
        self.sorted = config.executor_config.get('sorted')
        self.engine_type = config.executor_config.get('engine_type')
        self.actions = []

    @classmethod
    def about(cls):
        return '''
        Default Parser

        The default parser is automatically selected and used for all non action-set engines and non multi-sequence/multi-user-sequence
        personas. It offers both action sorting (based on an action's order attribute) for single sequences and easy access to action
        weights for collections of actions.

        '''

    def __len__(self):
        return len(self.actions)

    def __iter__(self):
        for action in self.actions:
            yield action

    def __getitem__(self, index):
        return self.actions[index]

    async def sort(self):
        actions = AsyncList(self.actions)
        sorted = await actions.sort(key=lambda action: action.order)
        return sorted

    async def weights(self):
        actions  = [action for action in self.actions if action.is_setup is False and action.is_teardown is False]
        return [action.weight if action.weight else 1 for action in actions]

    async def parse(self):
        for group in self._raw_actions:
            for action in self._raw_actions.get(group):
                action = Action(action, self.engine_type, group=group)
                action = action.type
                await action.parse_data()
                self.actions.append(action)

        return self.actions

