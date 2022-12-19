from asyncio import Task
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.personas.batching.batch import Batch
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook


class CompletedCounter:

    def __init__(self) -> None:
        self.completed_count = 0
        self.last_completed = 0
        self.last_batch_size = 0
        self.action = None

    async def execute_action(self, hook: Hook):
        response = await hook.session.execute_prepared_request(
            hook.action
        )
        
        
        self.completed_count += 1

        return response