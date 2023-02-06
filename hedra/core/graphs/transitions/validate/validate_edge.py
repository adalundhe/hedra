import asyncio
from typing import Dict, Any
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

        self.history['validation_stages'] = self.stages_by_type
        for event in self.source.dispatcher.events_by_name.values():
            if event.source.shortname in self.source.internal_events:
                event.context.update(self.history)
                
                if event.source.context:
                    event.source.context.update(self.history)
        
        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)

        else:
            await self.source.run()

        self._update(self.destination)
        
        self.destination.state = StageStates.VALIDATED


        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        self.next_history.update({
            destination.name: {}
        })