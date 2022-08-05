from hedra.core.engines.client import Client
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage



class Execute(Stage):
    stage_type=StageTypes.EXECUTE

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client = Client()
        self.accepted_hook_types = [ 
            HookType.SETUP, 
            HookType.BEFORE, 
            HookType.ACTION,
            HookType.AFTER,
            HookType.TEARDOWN,
            HookType.CHECK
        ]

    @Internal
    async def run(self):
        return await self.persona.execute()