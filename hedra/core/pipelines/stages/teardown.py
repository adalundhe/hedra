import asyncio
import inspect
from typing import Dict, List
from hedra.core.pipelines.hooks.registry.registrar import registar
from hedra.core.engines.client import Client
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN

    def __init__(self) -> None:
        super().__init__()
        self.actions = []
        self.hooks: Dict[HookType, List[Hook]] = {}

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def run(self):

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registar.all.get(method_name)

            if hook:
                self.hooks[hook.hook_type].append(hook)
        
        await asyncio.gather(*[
            hook.call() for hook in self.hooks.get(
                HookType.TEARDOWN
            )
        ])
