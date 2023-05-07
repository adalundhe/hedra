from __future__  import annotations
import inspect
import asyncio
import traceback
from collections import defaultdict
from typing import List, Dict, Any, Union
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.submit.submit import Submit
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.registrar import registrar
from hedra.reporting.experiment.experiment_metrics_set import ExperimentMetricsSet
from hedra.reporting.metric import MetricsSet
from hedra.reporting.metric.custom_metric import CustomMetric
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.reporting.metric.stage_metrics_summary import StageMetricsSummary
from hedra.reporting.metric.stage_streams_set import StageStreamsSet
from hedra.reporting.processed_result.types.base_processed_result import BaseProcessedResult


CustomMetricSet = Dict[str, Dict[str, CustomMetric]]
MetricsSetGroup = Dict[str, Union[str, Dict[str, StageMetricsSummary]]]


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
            'analyze_stage_session_total',
            'analyze_stage_events',
            'analyze_stage_summary_metrics',
        ]
        self.provides = [
            'submit_stage_metrics',
            'submit_stage_streamed_metrics',
            'submit_stage_experiment_metrics'
        ]

    async def transition(self):
        
        try:
            self.source.state = StageStates.SUBMITTING
            
            self.source.context.update(self.edge_data)

            for event in self.source.dispatcher.events_by_name.values():
                event.source.stage_instance = self.source
                event.context.update(self.edge_data)
                
                if event.source.context:
                    event.source.context.update(self.edge_data)

            if self.timeout and self.skip_stage is False:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)
            
            elif self.skip_stage is False:
                await self.source.run()
            
            for provided in self.provides:
                self.edge_data[provided] = self.source.context[provided]

            self._update(self.destination)

            self.source.state = StageStates.SUBMITTED
            self.destination.state = StageStates.SUBMITTED

            if self.destination.context is None:
                self.destination.context = SimpleContext()

            self.visited.append(self.source.name)

        except Exception as edge_exception:
            self.exception = edge_exception

        return None, self.destination.stage_type

    def _update(self, destination: Stage):

        for edge_name in self.history:

            history = self.history[edge_name]

            if self.next_history.get(edge_name) is None:
                self.next_history[edge_name] = {}

            self.next_history[edge_name].update(history)

        if self.skip_stage is False:
            self.next_history.update({
                (self.source.name, destination.name): {
                    'submit_stage_metrics': self.edge_data['submit_stage_metrics'],
                    'submit_stage_experiment_metrics': self.edge_data['submit_stage_experiment_metrics'],
                    'submit_stage_streamed_metrics': self.edge_data['submit_stage_streamed_metrics']
                }
            })

    def split(self, edges: List[SubmitEdge]) -> None:

        submit_stage_config: Dict[str, Any] = self.source.to_copy_dict()
        submit_stage_copy: Submit = type(self.source.name, (Submit, ), self.source.__dict__)()
        
        for copied_attribute_name, copied_attribute_value in submit_stage_config.items():
            if inspect.ismethod(copied_attribute_value) is False:
                setattr(
                    submit_stage_copy, 
                    copied_attribute_name, 
                    copied_attribute_value
                )

        user_hooks: Dict[str, Dict[str, Hook]] = defaultdict(dict)
        for hooks in registrar.all.values():
            for hook in hooks:
                if hasattr(self.source, hook.shortname) and not hasattr(Submit, hook.shortname):
                    user_hooks[self.source.name][hook.shortname] = hook._call
        
        submit_stage_copy.dispatcher = self.source.dispatcher.copy()

        for event in submit_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = submit_stage_copy

        submit_stage_copy.context = SimpleContext()
        for event in submit_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = submit_stage_copy 
            event.source.stage_instance.context = submit_stage_copy.context
            event.source.context = submit_stage_copy.context

            if event.source.shortname in user_hooks[submit_stage_copy.name]:
                hook_call = user_hooks[submit_stage_copy.name].get(event.source.shortname)

                hook_call = hook_call.__get__(submit_stage_copy, submit_stage_copy.__class__)
                setattr(submit_stage_copy, event.source.shortname, hook_call)

                event.source._call = hook_call

            else:            
                event.source._call = getattr(submit_stage_copy, event.source.shortname)
                event.source._call = event.source._call.__get__(submit_stage_copy, submit_stage_copy.__class__)
                setattr(submit_stage_copy, event.source.shortname, event.source._call)
 
        self.source = submit_stage_copy

        transition_idxs = [edge.transition_idx for edge in edges]
        
        if self.transition_idx != min(transition_idxs):
            self.skip_stage = True

    def setup(self):
        
        events: List[BaseProcessedResult] = []
        metrics: List[MetricsSet] = []
        streamed_metrics: Dict[str, StageStreamsSet] = {}
        experiments: List[ExperimentMetricsSet] = []
        session_total: int = 0

        for source_stage, destination_stage in self.history:
            if destination_stage == self.source.name:

                previous_history: Dict[str, Any] = self.history[(source_stage, self.source.name)]
            
                analyze_stage_summary_metrics: MetricsSetGroup = previous_history.get(
                    'analyze_stage_summary_metrics'
                )
                analyze_stage_events: List[BaseProcessedResult] = previous_history.get(
                    'analyze_stage_events',
                    []
                )

                events.extend(analyze_stage_events)


                stage_metrics_summaries = analyze_stage_summary_metrics.get('stages', {})
                for stage_metrics_summary in stage_metrics_summaries.values():
                    metrics.extend(list(
                        stage_metrics_summary.metrics_sets.values()
                    ))

                    session_total += stage_metrics_summary.stage_metrics.total

                experiment_metrics_sets = analyze_stage_summary_metrics.get('experiment_metrics_sets', {})
                experiments.extend(list(
                    experiment_metrics_sets.values()
                ))

                for stage_metrics_summary in stage_metrics_summaries.values():
                    streams = stage_metrics_summary.stage_streamed_analytics

                    if streams and len(streams) > 0:
                        streamed_metrics[stage_metrics_summary.stage_metrics.name] = stage_metrics_summary.streams

        self.edge_data['submit_stage_events'] = events
        self.edge_data['submit_stage_experiment_metrics'] = experiments
        self.edge_data['submit_stage_streamed_metrics'] = streamed_metrics
        self.edge_data['submit_stage_summary_metrics'] = metrics
        self.edge_data['submit_stage_session_total'] = session_total





