from hedra.core.engines.client import Client
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_states import StageStates
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage



class Execute(Stage):
    stage_type=StageTypes.EXECUTE

    def __init__(self) -> None:
        super().__init__()
        self.hooks = {}
        self.persona = None
        self.client = Client()

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def run(self):
        return await self.persona.execute()