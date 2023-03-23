from __future__  import annotations
import asyncio
from collections import defaultdict
from typing import List, Dict, Any
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.submit.submit import Submit
from hedra.reporting.metric import MetricsSet
from hedra.reporting.metric.custom_metric import CustomMetric
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates


CustomMetricSet = Dict[str, Dict[str, CustomMetric]]
MetricsSetGroup = Dict[str, Dict[str, Dict[str, Dict[str, MetricsSet]]]]


class SubmitEdge(BaseEdge[Submit]):

    def __init__(self, source: Submit, destination: BaseEdge[Stage]) -> None:
        super(
            SubmitEdge,
            self
        ).__init__(
            source,
            destination
        )
        
        self.requires = [
            'analyze_stage_custom_metrics_set',
            'analyze_stage_events',
            'analyze_stage_summary_metrics'
        ]
        self.provides = [
            'analyze_stage_summary_metrics'
        ]

    async def transition(self):
        self.source.state = StageStates.SUBMITTING
        
        history = self.history[(self.from_stage_name, self.source.name)]
        
        self.source.context.update(history)

        for event in self.source.dispatcher.events_by_name.values():
            event.source.stage_instance = self.source
            event.context.update(history)
            
            if event.source.context:
                event.source.context.update(history)

        if self.timeout and self.skip_stage is False:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        
        elif self.skip_stage is False:
            await self.source.run()
        
        for provided in self.provides:
            self.history[(self.from_stage_name, self.source.name)][provided] = self.source.context[provided]

        self._update(self.destination)

        self.source.state = StageStates.SUBMITTED
        self.destination.state = StageStates.SUBMITTED

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self.visited.append(self.source.name)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):

        if self.skip_stage:
            self.next_history.update({
                (self.source.name, destination.name): {
                    'analyze_stage_summary_metrics': {}
                }
            })

        else:
            history = self.history[(self.from_stage_name, self.source.name)]
            self.next_history.update({
                (self.source.name, destination.name): {
                    'analyze_stage_summary_metrics': history['analyze_stage_summary_metrics'] 
                }
            })

    def split(self, edges: List[SubmitEdge]) -> None:

        transition_idxs = [edge.transition_idx for edge in edges]
        
        if self.transition_idx != min(transition_idxs):
            self.skip_stage = True

    def merge(self):

        metrics: List[MetricsSet] = []
        for from_stage_name in self.from_stage_names:
            previous_history = self.history[(from_stage_name, self.source.name)]
            analyze_stage_summary_metrics: MetricsSetGroup = previous_history['analyze_stage_summary_metrics']
            stage_summaries = analyze_stage_summary_metrics.get('stages', {})
            for stage in stage_summaries.values():
                metrics.extend(list(
                    stage.get('actions', {}).values()
                ))

        metrics_set_counts = defaultdict(lambda: 0)

        for metrics_set in metrics:
            metrics_set_counts[metrics_set.name] += 1

        custom_metrics: Dict[str, CustomMetric] = {}
        for from_stage in self.from_stage_names:
            previous_history = self.history[(from_stage, self.source.name)]
            analyze_stage_custom_metrics_set: CustomMetricSet = previous_history['analyze_stage_custom_metrics_set']
            
            for custom_metrics_set in analyze_stage_custom_metrics_set.values():
                custom_metrics.update(custom_metrics_set)

    
        for metric_set in metrics:
            set_count = metrics_set_counts.get(metric_set.name)

            if set_count > 1:
                # If two of the same set exist we merge their custom metrics.
                metric_set.custom_metrics = custom_metrics





