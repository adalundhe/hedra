from typing import Union
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    TaskHook
)



class CompletedCounter:

    def __init__(self) -> None:
        self.completed_count = 0
        self.last_completed = 0
        self.last_batch_size = 0
        self.action = None

    async def execute_action(self, hook: Union[ActionHook, TaskHook]):
        response = await hook.session.execute_prepared_request(
            hook.action
        )
        
        
        self.completed_count += 1

        return response