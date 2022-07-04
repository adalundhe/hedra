import inspect
from types import FunctionType
from typing import Coroutine
from easy_logger import Logger
from async_tools.datatypes import AsyncList

class ActionsParser:

    def __init__(self, config):
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self._raw_actions = config.actions
        self.sorted = config.executor_config.get('sorted')
        self._is_multi_sequence = config.executor_config.get('persona_type') == 'multi-sequence'
        self._is_multi_user_sequence = config.executor_config.get('persona_type') == 'multi-user-sequence'
        self.actions = []
        self.sequences = {}
        self.before = []
        self.after = []
        self.user_setup_actions = {}
        self.user_teardown_actions = {}
        self.setup_actions = []
        self.teardown_actions = []
        self.engine_type = config.executor_config.get('engine_type')

    @classmethod
    def about(cls):

        registered_parsers = '\n\t'.join([ f'- {parser_type}' for parser_type in cls.parsers ])

        return f'''
        Action Parsers

        key-arguments:

        --engine (The engine type to use - determines the action type the parser will use)
        --persona (The parser type to use - determines the parser used)

        Actions, whether written Python or read-in as JSON file data, need to be parsed prior to
        execution so Personas can easily organize and Engines easily execute them. Action parsers 
        are responsible for handling this task, allowing Hedra to implement a consistent interface
        both for declaring/describing actions and for processing/consuming actions internally.

        All parsers share the following methods:

        - parse (maps raw action data to the required format for Hedra to consume during execution)

        Currently registered parsers include:

        {registered_parsers}
        
        For more information on parsers, run the command:

            hedra --about actions:parsers:<parser_type>


        Related Topics:

        -personas
        -engines
        -actions

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

    async def _parse(self, class_instance):
        methods = inspect.getmembers(class_instance, predicate=inspect.ismethod) 

        user = type(class_instance).__name__
            
        if self._is_multi_sequence:

            for _, method in methods:

                if hasattr(method, 'is_action'):

                    group = method.metadata.get('group')
                    if group is None:
                        group = user
                    
                    if self.sequences.get(group) is None:
                        self.sequences[group] = {
                            'setup': [],
                            'teardown': []
                        }

                    if method.is_setup:
                        self.sequences[group]['setup'].append(method)

                    elif method.is_teardown:
                        self.sequences[group]['teardown'].append(method)

                    elif method.is_before:
                        self.before.append(method)

                    elif method.is_after:
                        self.after.append(method)

                    else:
                        if self.sequences[group].get('execute') is None:
                            self.sequences[group]['execute'] = [method]

                        else:
                            self.sequences[group]['execute'].append(method)
        
        else:
            for _, method in methods:
                if hasattr(method, 'is_action'):
                    
                    if method.is_setup:
                        self.setup_actions.append(method)

                    elif method.is_teardown:
                        self.teardown_actions.append(method)

                    elif method.is_before:
                        self.before.append(method)

                    elif method.is_after:
                        self.after.append(method)

                    else:
                        self.actions.append(method)

    async def setup(self, action: Coroutine):
        
        before_hook = None
        for hook in self.before:
            if action.name in hook.names:
                before_hook = hook

        after_hook = None
        for hook in self.after:

            if action.name in hook.names:
                after_hook = hook

        parsed_action = await action()
        parsed_action.data.before = before_hook
        parsed_action.data.after = after_hook

        return parsed_action

    async def weights(self):
        actions  = [action for action in self.actions if action.is_setup is False and action.is_teardown is False]
        return [action.weight if action.weight else 1 for action in actions]

    async def sort(self):
        if self._is_multi_sequence:
            return await self.sort_multi_sequence()

        else:
            return await self.sort_sequence()

    async def sort_multi_sequence(self):
        sorted_sequences = {}
        for sequence_name, sequence in self.sequences.items():
            sorted_sequences[sequence_name] = sequence
            unsorted_sequence = self.sequences[sequence_name].get('execute')

            if unsorted_sequence:
                sorted_sequence = AsyncList(unsorted_sequence)
                sorted_sequence = await sorted_sequence.sort(key=lambda action: action.order)

                sorted_sequences[sequence_name]['execute'] = sorted_sequence

        return sorted_sequences

    async def sort_sequence(self):
        actions = AsyncList(self.actions)
        return await actions.sort(key=lambda action: action.order)