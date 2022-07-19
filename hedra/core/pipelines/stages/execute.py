from hedra.core.hooks.client import Client
from hedra.core.hooks.types.types import HookType
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

    async def run(self):

        results = []

        if self.state == StageStates.SETUP:

            self.state = StageStates.EXECUTING
            results = await self.persona.execute()

            self.state = StageStates.EXECUTED

        return results