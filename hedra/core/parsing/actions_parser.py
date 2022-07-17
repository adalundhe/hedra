# from typing import Dict, List, Union
# from easy_logger import Logger
# from hedra.core.engines.types.common.hooks import Hooks
# from hedra.test.hooks.hook import Hook
# from hedra.test.hooks.types import HookType
# from hedra.core.pipelines.stages.execute import Execute


# class ActionsParser:

#     def __init__(self, config):
#         logger = Logger()
#         self.session_logger = logger.generate_logger('hedra')
#         self._raw_actions = config.actions
#         self.sorted = config.executor_config.get('sorted')
#         self._is_multi_sequence = config.executor_config.get('persona_type') == 'multi-sequence'
#         self._is_multi_user_sequence = config.executor_config.get('persona_type') == 'multi-user-sequence'
        # self.action_sets: Dict[str, Execute] = {}
        # self.actions: Union[List, Dict] = []
        # self.hooks = {}
        # self.setup_actions = []
        # self.teardown_actions = []
        # self.engine_type = config.executor_config.get('engine_type')

    # def __len__(self):
    #     return len(self.actions)

    # def __iter__(self):
    #     for action in self.actions:
    #         yield action

    # def __getitem__(self, index):
    #     return self.actions[index]

    # async def parse(self):
    #     for python_class in self._raw_actions:
    #         class_instance: Execute = python_class()
    #         await class_instance.register_actions()

    #         self.hooks[class_instance.name] = class_instance.hooks

    #         self.action_sets[type(class_instance).__name__] = class_instance

    # def weights(self):
    #     weighted_actions = []

    #     for action_set in self.action_sets.values():
    #         actions: List[Hook] = action_set.actions
    #         weighted_actions.extend(actions)

    #     self.actions = [
    #         (
    #             idx, 
    #             action, 
    #             action.config.weight
    #         ) for idx, action in enumerate(actions)
    #     ]

    # def sort_multisequence(self):
    #     self.actions = {}
    #     for action_set_name, action_set in self.action_sets.items():
    #         actions: List[Hook] = action_set.actions
    #         sorted_set = sorted(actions, key=lambda action: action.config.order)
    #         action_set.actions = list(sorted_set)
            
    #         self.actions[action_set_name].hooks[HookType.ACTION] = actions

    # def sort_sequence(self):

    #     sorted_actions = []

    #     for action_set in self.action_sets.values():
    #         actions: List[Hook] = action_set.actions
    #         sorted_set = sorted(actions, key= lambda action: action.config.order)

    #         sorted_actions.extend(
    #             list(sorted_set)
    #         )

    #     self.actions = sorted_actions