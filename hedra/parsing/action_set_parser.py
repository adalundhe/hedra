import inspect
from easy_logger import Logger
from hedra.core.engines import engine

from .actions import Action
from async_tools.datatypes import AsyncList
from .multi_user_sequence_persona import MultiUserSequenceParser
from .multi_user_sequence_persona import MultiUserSequenceParser

class ActionSetParser(MultiUserSequenceParser):

    def __init__(self, config):
        super(ActionSetParser, self).__init__(config)
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self._raw_actions = config.actions
        self.sorted = config.executor_config.get('sorted')
        self._is_multi_sequence = config.executor_config.get('persona_type') == 'multi-sequence'
        self._is_multi_user_sequence = config.executor_config.get('persona_type') == 'multi-user-sequence'
        self.actions = []
        self.sequences = {}
        self.user_setup_actions = {}
        self.user_teardown_actions = {}
        self.setup_actions = []
        self.teardown_actions = []
        self.engine_type = config.executor_config.get('engine_type')

    @classmethod
    def about(cls):
        return '''
        Action Set Parser - (action-set)

        The Action Set parser is automatically selected and used when the Action Set engine is selected, mapping
        actions defined by Python code via Hedra' Testing package. This differs from other parsers, which are selected 
        based on the Persona selected. However, the Action Set parser supports all personas that other parsers
        support.

        '''

    def __len__(self):
        return len(self.actions)

    def __iter__(self):
        for action in self.actions:
            yield action

    def __getitem__(self, index):
        return self.actions[index]

    async def parse(self):
        for python_class in self._raw_actions:
            class_instance = python_class()

            await self._parse(class_instance)

        if self._is_multi_user_sequence or self._is_multi_sequence:
            self.actions = self.sequences
            
        return self.actions

    async def _parse(self, class_instance):
        methods = inspect.getmembers(class_instance, predicate=inspect.ismethod) 

        user = type(class_instance).__name__

        if self._is_multi_user_sequence:


            if self.sequences.get(user) is None:
                self.sequences[user] = {}
                self.user_setup_actions[user] = []
                self.user_teardown_actions[user] = []

            sequences = {}
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    action = await self._parse_action(
                        method, 
                        user=user
                    )

                    if action.is_setup:
                        self.user_setup_actions[user].append(action)

                    elif action.is_teardown:
                        self.user_teardown_actions[user].append(action)

                    else:
                        if sequences.get(action.group) is None and action.group != user:
                            sequences[action.group] = [action]

                        elif action.group != user:
                            sequences[action.group].append(action)

            self.sequences[user] = sequences
            
        elif self._is_multi_sequence:
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    action = await self._parse_action(
                        method, 
                        user=class_instance.name
                    )

                    if action.is_setup:
                        self.setup_actions.append(action)

                    elif action.is_teardown:
                        self.teardown_actions.append(action)

                    else:
                        if self.sequences.get(action.group) is None and action.group != user:
                            self.sequences[action.group] = [action]

                        elif action.group != user:
                            self.sequences[action.group].append(action)
        
        else:
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    action = await self._parse_action(
                        method, 
                        user=class_instance.name
                    )
                    
                    if action.is_setup:
                        self.setup_actions.append(action)

                    elif action.is_teardown:
                        self.teardown_actions.append(action)

                    else:
                        self.actions.append(action)

    async def _parse_action(self, method, user=None):
        action = Action(
            {
                'name': method.action_name,
                'weight': method.weight,
                'order': method.order,
                'env': method.env,
                'tags': method.tags,
                'url': method.url,
                'env': method.env,
                'type': method.type,
                'timeout': method.timeout,
                'wait_interval': method.wait_interval,
                'user': user,
                'action': method,
                'checks': method.checks
            },
            'action-set',
            group=method.group
        )

        action = action.type
        await action.parse_data()

        return action

    async def weights(self):
        actions  = [action for action in self.actions if action.is_setup is False and action.is_teardown is False]
        return [action.weight if action.weight else 1 for action in actions]

    async def sort(self):
        if self._is_multi_user_sequence:
            return await self.sort_multi_user_sequence()

        elif self._is_multi_sequence:
            return await self.sort_multi_sequence()

        else:
            return await self.sort_sequence()

    async def sort_multi_user_sequence(self):
        sorted_sequences = {}
        
        for user in self.sequences:

            sorted_user_sequences = {}

            for sequence in self.sequences[user]:
                unsorted_sequence = self.sequences[user][sequence]
                sorted_sequence = AsyncList(unsorted_sequence)

                sorted_sequence = await sorted_sequence.sort(key=lambda action: action.order)

                sorted_user_sequences[sequence] = sorted_sequence

            sorted_sequences[user] = sorted_user_sequences
        
        return sorted_sequences

    async def sort_multi_sequence(self):
        sorted_sequences = {}
        for sequence in self.sequences:
            unsorted_sequence = self.sequences[sequence]
            sorted_sequence = AsyncList(unsorted_sequence)

            sorted_sequence = await sorted_sequence.sort(key=lambda action: action.order)

            sorted_sequences[sequence] = sorted_sequence

        return sorted_sequences

    async def sort_sequence(self):
        actions = AsyncList(self.actions)
        return await actions.sort(key=lambda action: action.order)
 