import asyncio
import inspect
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.hook_types import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from .stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN

    def __init__(self) -> None:
        super().__init__()
        self.actions = []

    @Internal()
    async def run(self):

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting Teardown stage.')

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registrar.all.get(method_name)

            if hook:
                self.hooks[hook.hook_type].append(hook)

        teardown_hooks = self.hooks.get(HookType.TEARDOWN)

        if teardown_hooks:

            teardown_hook_names = ', '.join([hook.name for hook in teardown_hooks])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Running teardown hooks - {teardown_hook_names}')           
            await asyncio.gather(*[ hook.call() for hook in teardown_hooks])

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Teardown complete.')
