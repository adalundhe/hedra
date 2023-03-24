import os
from typing import Dict, List, Any
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.stages import Checkpoint
from hedra.core.hooks.types import context
from hedra.core.hooks.types import save


class CheckpointStage(Checkpoint):

    @save(save_path=f'{os.getcwd()}/checkpoint.json')
    async def save_results(
        self, 
        results: List[BaseResult]=[]
    ) -> List[Dict[str, Any]]:
        return [
            {
                'action_id': data.action_id,
                'elapsed': data.complete - data.start
            } for data in results
        ]