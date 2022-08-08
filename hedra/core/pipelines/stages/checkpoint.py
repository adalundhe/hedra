from datetime import datetime
import json
import os
from typing import List, Optional
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage

class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT

    def __init__(self) -> None:
        super().__init__()
        self.data = {}
        self.previous_stage = ''
        self.accepted_hook_types = [ HookType.SAVE ]

    @Internal
    async def run(self):
        
        for save_hook in self.hooks.get(HookType.SAVE):
            checkpoint_data = await save_hook.call(self.data)

            with open(save_hook.config.path, 'w') as checkpoint_file:
                json.dump(checkpoint_data, checkpoint_file, indent=4)