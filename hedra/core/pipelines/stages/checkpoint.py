from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage

class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT

    def __init__(self) -> None:
        super().__init__()

    async def run(self):
        summaries = self.context.summaries
        reporting_config = self.context.reporting_config

        #TODO: Checkpointing done here.
        print(summaries)