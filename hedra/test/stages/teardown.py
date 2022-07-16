from typing import Dict, List
from hedra.test.client import Client
from hedra.test.hooks.hook import Hook
from hedra.test.hooks.types import HookType
from hedra.test.stages.types.stage_types import StageTypes
from .stage import Stage


class Teardown(Stage):
    stage_type=StageTypes.TEARDOWN
    stage_order=4
    client: Client = None

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