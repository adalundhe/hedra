from typing import Dict, List
from hedra.core.hooks.client import Client
from hedra.core.hooks.types.hook import Hook
from hedra.core.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN

    def __init__(self) -> None:
        super().__init__()
        self.actions = []
        self.hooks: Dict[str, List[Hook]] = {}

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def teardown(self):
        for teardown_hook in self.hooks.get(HookType.TEARDOWN):

            try:
                
                await teardown_hook()

            except Exception:
                pass