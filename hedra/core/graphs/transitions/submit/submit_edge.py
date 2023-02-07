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
            'analyze_stage_events': [],
            'analyze_stage_summary_metrics': {}
        }

        self.requires = [
            'analyze_stage_events',
            'analyze_stage_summary_metrics'
        ]
        self.provides = [
            'analyze_stage_summary_metrics'
        ]

    async def transition(self):
        self.source.state = StageStates.SUBMITTING

        history = self.history[self.from_stage_name]

        for event in self.source.dispatcher.events_by_name.values():
            self.source.context.update(history)
            event.context.update(history)
            
            if event.source.context:
                event.source.context.update(history)

        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        
        else:
            await self.source.run()
        
        for provided in self.provides:
            self.history[self.from_stage_name][provided] = self.source.context[provided]

        self._update(self.destination)

        self.source.state = StageStates.SUBMITTED
        self.destination.state = StageStates.SUBMITTED

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self.visited.append(self.source.name)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        history = self.history[self.from_stage_name]
        self.next_history.update({
            destination.name: {
                'analyze_stage_summary_metrics': history['analyze_stage_summary_metrics'] 
            }
        })