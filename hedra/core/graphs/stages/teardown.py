import asyncio
import inspect
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.types import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from .stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN

    def __init__(self) -> None:
        super().__init__()
        self.actions = []

    @Internal
    async def run(self):

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registrar.all.get(method_name)

            if hook:
                self.hooks[hook.hook_type].append(hook)
        
        await asyncio.gather(*[
            hook.call() for hook in self.hooks.get(
                HookType.TEARDOWN,
                []
            )
        ])
