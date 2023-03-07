from __future__ import annotations
import asyncio
from typing import List
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.checkpoint.checkpoint import Checkpoint
from hedra.core.graphs.stages.types.stage_states import StageStates

class CheckpointEdge(BaseEdge[Checkpoint]):

    def __init__(self, source: Checkpoint, destination: BaseEdge[Stage]) -> None:
        super(
            CheckpointEdge,
            self
        ).__init__(
            source,
            destination
        )

        self.requires = [
            'setup_stage_ready_stages',
            'setup_stage_candidates',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'execute_stage_setup_hooks',
            'execute_stage_results'
        ]

        self.provides = [
            'execute_stage_results'
        ]

    async def transition(self):
        self.source.state = StageStates.CHECKPOINTING

        history = self.history[(self.from_stage_name, self.source.name)]

        await self.source.context.update(history)
        
        if self.timeout and self.skip_stage is False:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        
        elif self.skip_stage is False:
            await self.source.run()

        for provided in self.provides:
            history[provided] = self.source.context[provided]

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self._update(self.destination)

        self.source.state = StageStates.CHECKPOINTED

        self.visited.append(self.source.name)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):

        for edge_name in self.history:

            history = self.history[edge_name]

            if self.next_history.get(edge_name) is None:
                self.next_history[edge_name] = {}

            self.next_history[edge_name].update({
                key: value for key, value  in history.items() if key in self.provides
            })

        if self.next_history.get((self.source.name, destination.name)) is None:
            self.next_history[(self.source.name, destination.name)] = {}

        if self.skip_stage:
            self.next_history.update({
                (self.source.name, destination.name): {}
            })

        else:
            self.next_history.update({
                (self.source.name, destination.name): {}
            })

    def split(self, edges: List[CheckpointEdge]) -> None:
        pass



