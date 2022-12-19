import os
from typing import Dict, List, Any
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.stages import Checkpoint
from hedra.core.graphs.hooks.hook_types import save


class CheckpointStage(Checkpoint):

    @save(f'{os.getcwd()}/checkpoint.json')
    async def save_results(self, data: List[BaseResult]) -> List[Dict[str, Any]]:
        return [
            {
                'action_id': data.action_id,
                'elapsed': data.complete - data.start
            } for data in data
        ]