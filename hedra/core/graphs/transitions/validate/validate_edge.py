import asyncio
from typing import List
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.validate.validate import Validate
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


class ValidateEdge(BaseEdge[Validate]):

    def __init__(self, source: Validate, destination: Stage) -> None:
        super(
            ValidateEdge,
            self
        ).__init__(
            source,
            destination
        )

    async def transition(self):
        self.source.state = StageStates.VALIDATING

        history = self.history[(self.from_stage_name, self.source.name)]


        history['validation_stages'] = self.stages_by_type
        for event in self.source.dispatcher.events_by_name.values():
            if event.source.shortname in self.source.internal_events:
                self.source.context.update(history)
                event.context.update(history)
                
                if event.source.context:
                    event.source.context.update(history)
        
        if self.timeout and self.skip_stage is False:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)

        elif self.skip_stage is False:
            await self.source.run()

        self._update(self.destination)
        
        self.destination.state = StageStates.VALIDATED

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        self.next_history.update({
            (self.source.name, destination.name): {}
        })

    def split(self, edges: List[BaseEdge]) -> None:
        pass