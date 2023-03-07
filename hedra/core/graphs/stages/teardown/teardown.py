from hedra.core.graphs.hooks.hook_types.internal import Internal
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
        await self.dispatcher.dispatch_events(self.name)
