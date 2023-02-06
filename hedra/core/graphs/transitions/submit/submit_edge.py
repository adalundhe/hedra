import asyncio
from typing import Dict, Any
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.submit.submit import Submit
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates


class SubmitEdge(BaseEdge[Submit]):

    def __init__(self, source: Submit, destination: BaseEdge[Stage]) -> None:
        super(
            SubmitEdge,
            self
        ).__init__(
            source,
            destination
        )

        self.history = {
            'summaries': {}
        }

        self.requires = [
            'summaries'
        ]
        self.provides = [
            'summaries'
        ]

    async def transition(self):
        self.source.state = StageStates.SUBMITTING

        for event in self.source.dispatcher.events_by_name.values():
            if event.source.shortname in self.source.internal_events:
                event.context.update(self.history)
                
                if event.source.context:
                    event.source.context.update(self.history)

        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        
        else:
            await self.source.run()
        
        for provided in self.provides:
            self.history[provided] = self.source.context[provided]

        self._update(self.destination)

        self.source.state = StageStates.SUBMITTED
        self.destination.state = StageStates.SUBMITTED

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        self.next_history.update({
            destination.name: {
                'summaries': self.history['summaries'] 
            }
        })