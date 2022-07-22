from typing import Dict
from hedra.core.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Submit(Stage):
    stage_type=StageTypes.SUBMIT
    
    def __init__(self) -> None:
        super().__init__()
        self.summaries = {}
        self.reporting_config = {}
        self.hooks: Dict[str, HookType] = {}

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def run(self):
        print(self.summaries)
