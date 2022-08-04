from datetime import datetime
import json
from typing import Optional
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage

class Checkpoint(Stage):
    stage_type=StageTypes.CHECKPOINT
    checkpoint_path: Optional[str]=None

    def __init__(self) -> None:
        super().__init__()
        self.data = {}
        self.previous_stage = ''

    @Internal
    async def run(self):
        
        if self.checkpoint_path is None:
            current_time = datetime.now()
            time_as_string = current_time.strftime('%Y-%m-%dT%H:%M:%S.Z')

            self.checkpoint_path = f'checkpoint_{self.previous_stage.lower()}_{time_as_string}.json'

            with open(self.checkpoint_path, 'w') as checkpoint_file:
                json.dump(self.data, checkpoint_file, indent=4)