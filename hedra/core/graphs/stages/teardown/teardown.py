import asyncio
import inspect
from typing import List
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.hook_types.event import event  
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.registry.registry_types import TeardownHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.stages.base.stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN

    def __init__(self) -> None:
        super().__init__()
        self.actions = []
        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.TRANSFORM,
            HookType.TEARDOWN
        ]

    @Internal()
    async def run(self):
        await self.setup_events()
        await self.dispatcher.dispatch_events()

    @event()
    async def collect_teardown_hooks(self):
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting Teardown stage.')

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook_set: List[Hook] = registrar.all.get(method_name, [])

            for hook in hook_set:
                self.hooks[hook.hook_type].append(hook)

        teardown_hooks: List[TeardownHook] = self.hooks[HookType.TEARDOWN]

        return {
            'teardown_hooks': teardown_hooks
        }

    @event('collect_teardown_hooks')
    async def run_teardown_hooks(
        self,
        teardown_hooks: List[TeardownHook]=[]
    ):

        if teardown_hooks:

            teardown_hook_names = ', '.join([hook.name for hook in teardown_hooks])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Running teardown hooks - {teardown_hook_names}')           
            results = await asyncio.gather(*[teardown_hook.call(self.context) for teardown_hook in teardown_hooks])
            for result in results:
                self.context = result

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Teardown complete.')
