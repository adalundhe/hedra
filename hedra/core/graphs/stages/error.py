import asyncio
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.hooks.types.internal import Internal
from .stage import Stage

class Error(Stage):
    stage_type=StageTypes.ERROR

    def __init__(self) -> None:
        super().__init__()
        self.error = None

        internal_hook = self.run()
        internal_hook.call = internal_hook.call.__get__(
            self, 
            self.__class__
        )
        setattr(self, internal_hook.shortname, internal_hook.call)

    @Internal
    async def run(self):
        pass

