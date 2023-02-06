import asyncio
from typing import Dict, Any
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.analyze.analyze import Analyze
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


class AnalyzeEdge(BaseEdge[Analyze]):

    def __init__(self, source: Analyze, destination: BaseEdge[Stage]) -> None:
        super(
            AnalyzeEdge,
            self
        ).__init__(
            source,
            destination
        )

        self.history = {
            'results': {}
        }

        self.requires = [
            'results'
        ]
        self.provides = [
            'summaries'
        ]

        self.valid_states = [
            StageStates.EXECUTED, 
            StageStates.CHECKPOINTED, 
            StageStates.TEARDOWN_COMPLETE
        ]
    
    async def transition(self):
        self.source.state = StageStates.ANALYZING

        raw_results = dict(self.history.get('results', {}))
        execute_stages = self.stages_by_type.get(StageTypes.EXECUTE)
        submit_stages = self.stages_by_type.get(StageTypes.SUBMIT)

        results_to_calculate = {}
        target_stages = {}
        for stage_name in raw_results.keys():
            stage = execute_stages.get(stage_name)

            in_path = self.source.name in self.all_paths.get(stage.name)

            if stage.state in self.valid_states and in_path:
                stage.state = StageStates.ANALYZING
                results_to_calculate[stage_name] = raw_results.get(stage_name)
                target_stages[stage_name] = stage
        
        self.history['raw_results'] = results_to_calculate
        self.history['target_stages'] = target_stages
        
        for event in self.source.dispatcher.events_by_name.values():
            if event.source.shortname in self.source.internal_events:
                event.context.update(self.history)
                
                if event.source.context:
                    event.source.context.update(self.history)
                    
        if len(results_to_calculate) > 0:

            if self.timeout:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)

            else:
                await self.source.run()

        self.history['summary_metrics'] = self.source.context['summary_metrics']

        self.destination.state = StageStates.ANALYZED

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        if len(results_to_calculate) > 0:

            self._update(self.destination)
 
            for stage in submit_stages.values():
                if stage.name in self.all_paths.get(self.source.name) and stage.state == StageStates.INITIALIZED:

                    if stage.context is None:
                        stage.context = SimpleContext()

                    self._update(stage)

                    stage.state = StageStates.ANALYZED

        self.source.state = StageStates.ANALYZED
        self.visited.append(self.source.name)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):
        self.next_history.update({
            destination.name: {
                'summaries': self.history.get('summary_metrics', {})
            }
        })