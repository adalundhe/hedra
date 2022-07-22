import asyncio
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Complete(Stage):
    stage_type=StageTypes.COMPLETE

    def __init__(self) -> None:
        super().__init__()

    async def run(self):
        pending = asyncio.all_tasks()

        for pend in pending:
            try:
                pend.cancel()
                await pend
            except Exception:
                pass