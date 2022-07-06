import inspect
from typing import Coroutine, Dict, List
from easy_logger import Logger
from async_tools.datatypes import AsyncList
from hedra.test.actions.base import Action
from hedra.test.hooks.types import HookType
from hedra.test.stages.execute import Execute


class ActionsParser:

    def __init__(self, config):
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self._raw_actions = config.actions
        self.sorted = config.executor_config.get('sorted')
        self._is_multi_sequence = config.executor_config.get('persona_type') == 'multi-sequence'
        self._is_multi_user_sequence = config.executor_config.get('persona_type') == 'multi-user-sequence'
        self.actions: Dict[str, Execute] = {}
        self.sequences = {}
        self.hooks = {
            HookType.BEFORE: [],
            HookType.AFTER: [],
            HookType.BEFORE_BATCH: [],
            HookType.AFTER_BATCH: []
        }
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
            class_instance: Execute = python_class()
            await class_instance.register_actions()

            for action in class_instance.registry:
                class_instance.registry[action.parsed.name] = self.set_hooks(action)

            self.actions[type(class_instance).__name__] = class_instance

    def set_hooks(self, action: Action):
        
        action.before_batch = self.get_hook(action, HookType.BEFORE_BATCH)
        action.after_batch = self.get_hook(action, HookType.AFTER_BATCH)
        action.before = self.get_hook(action, HookType.BEFORE)
        action.after = self.get_hook(action, HookType.AFTER)

        return action

    def get_hook(self, action, hook_type):
        for hook in self.hooks[hook_type]:
            if action.name in hook.names:
                return hook

    async def weights(self):
        actions  = [action for action in self.actions if action.is_setup is False and action.is_teardown is False]
        return [action.weight if action.weight else 1 for action in actions]

    async def sort(self):
        if self._is_multi_sequence:
            return await self.sort_multi_sequence()

        else:
            return await self.sort_sequence()

    async def sort_multi_sequence(self):

        for action_set_name, action_set in self.actions.items():
            actions = action_set.registry.to_list()
            sorted_actions = sorted(actions, key=lambda action: action.order)
            action_set.registry.data = list(sorted_actions)
            
            self.actions[action_set_name] = actions

    async def sort_sequence(self):
        actions = []
        for action_set in self.actions.values():
            registry_actions = action_set.registry.to_list()
            sorted_actions = sorted(registry_actions, key=lambda action: action.order)
            actions.extend(list(sorted_actions))

        return actions