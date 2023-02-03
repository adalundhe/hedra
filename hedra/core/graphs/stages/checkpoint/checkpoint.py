import asyncio
from typing import List
from hedra.core.graphs.hooks.hook_types.event import event
from hedra.core.graphs.hooks.registry.registry_types import (
    SaveHook,
    LoadHook
)
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.stages.base.stage import Stage


class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT

    def __init__(self) -> None:
        super().__init__()
        self.previous_stage = None
        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.LOAD,
            HookType.SAVE, 
            HookType.TRANSFORM
        ]

        self.requires_shutdown = True

    @Internal()
    async def run(self):
        await self.setup_events()
        await self.dispatcher.dispatch_events()
