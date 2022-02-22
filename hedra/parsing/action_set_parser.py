import inspect
from zebra_automate_logging import Logger
from hedra.execution.engines import engine

from .actions import Action
from zebra_async_tools.datatypes import AsyncList
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

            if self._is_multi_user_sequence:
                pass
            
        return self.actions

    async def _parse(self, class_instance):
        methods = inspect.getmembers(class_instance, predicate=inspect.ismethod) 

        if self._is_multi_user_sequence:

            user = class_instance.name

            self.sequences[user] = {}
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    action = await self._parse_action(
                        method, 
                        user=user
                    )
                    group = self.sequences.get(action.group)

                    if group is None:
                        self.sequences[user][action.group] = [action]
                    else:
                        self.sequences[user][action.group].append(action)

        elif self._is_multi_sequence:
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    action = await self._parse_action(
                        method, 
                        user=class_instance.name
                    )
                    group = self.sequences.get(action.group)

                    if group is None:
                        self.sequences[action.group] = [action]
                    else:
                        self.sequences[action.group].append(action)
        
        else:
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    action = await self._parse_action(
                        method, 
                        user=class_instance.name
                    )
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
                'user': user,
                'action': method,
                'success_condition': method.success_condition
            },
            'action-set',
            group=method.group
        )

        action = action.type

        return action

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

    async def sort_multi_sequence(self):
        sorted_sequences = {}
        for sequence in self.sequences:
            unsorted_sequence = self.sequences[sequence]
            sorted_sequence = AsyncList(unsorted_sequence)

            sorted_sequence = await sorted_sequence.sort(key=lambda action: action.order)

            sorted_sequences[sequence] = sorted_sequence

        return sorted_sequences

    async def sort_sequence(self):
        actions = AsyncList(self.parser.actions)
        return await actions.sort(key=lambda action: action.order)




 