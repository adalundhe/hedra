from typing import Coroutine, Dict, List
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common.types import RequestTypes


class Task(BaseAction):

    __slots__ = (
        'action_id',
        'protocols', 
        'name', 
        'is_setup', 
        'metadata', 
        'hooks',
        'type',
        'source',
        'execute'
    )

    def __init__(
        self, 
        name: str,
        task_action: Coroutine,
        source: str=None,
        user: str=None, 
        tags: List[Dict[str, str]] = []
    ):
        super(Task, self).__init__(
            name,
            user,
            tags
        )

        self.type = RequestTypes.TASK
        self.source = source
        self.execute = task_action
        self.hooks: Hooks[Task] = Hooks()

    def to_serializable(self):

        return {
            'name': self.name,
            'type': self.type,
            'metadata': {
                'user': self.metadata.user,
                'tags': self.metadata.tags
            },
            'hooks': self.hooks.to_serializable()
        }