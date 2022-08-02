from typing import Dict
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Validate(Stage):
    stage_type=StageTypes.VALIDATE
    
    def __init__(self) -> None:
        super().__init__()
        self.hooks: Dict[str, HookType] = {}

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def run(self):
        pass