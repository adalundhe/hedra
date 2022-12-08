from typing import List, Union
from .actions import (
    Syncrhonize,
    Initialize,
    RepoConfig
)

from .exceptions import InvalidActionError


class GraphManager:

    def __init__(self) -> None:
        self._actions = {
            'initialize': Initialize,
            'synchronize': Syncrhonize   
        }

    def execute_workflow(self, workflow_actions: List[str], config: RepoConfig):
        
        for workflow_action in workflow_actions:
            action: Union[Initialize, Syncrhonize] = self._actions.get(workflow_action)(config)

            if action is None:
                raise InvalidActionError(
                    workflow_action, 
                    list(self._actions.keys())
                )

            action.execute()