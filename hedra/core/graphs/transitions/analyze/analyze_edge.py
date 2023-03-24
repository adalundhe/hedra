from __future__ import annotations
import asyncio
import inspect
from typing import Dict, List, Any
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.submit.submit import Submit
from hedra.core.graphs.stages.analyze.analyze import Analyze
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

        self.requires = [
            'execute_stage_results'
        ]
        self.provides = [
            'analyze_stage_custom_metrics_set',
            'analyze_stage_summary_metrics'
        ]

        self.valid_states = [
            StageStates.EXECUTED, 
            StageStates.CHECKPOINTED, 
            StageStates.TEARDOWN_COMPLETE
        ]
        
        self.assigned_candidates = []
    
    async def transition(self):
        self.source.state = StageStates.ANALYZING
        submit_candidates = self.generate_submit_candidates()

        if len(self.assigned_candidates) > 0:
            submit_candidates = {
                stage_name: stage for stage_name, stage in submit_candidates.items() if stage_name in self.assigned_candidates
            }
        
        self.source.context.update(self.edge_data)
        
        for event in self.source.dispatcher.events_by_name.values():
            event.source.stage_instance = self.source
            event.context.update(self.edge_data)
            
            if event.source.context:
                event.source.context.update(self.edge_data)
        

        if self.edge_data['analyze_stage_has_results']:

            if self.timeout and self.skip_stage is False:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)

            elif self.skip_stage is False:
                await self.source.run()
        
        for provided in self.provides:
            self.edge_data[provided] = self.source.context[provided]

        self.destination.state = StageStates.ANALYZED

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        if self.edge_data['analyze_stage_has_results']:

            self._update(self.destination)

            all_paths = []
            for path_set in self.all_paths.get(self.source.name, []):
                all_paths.extend(path_set)
 
            for stage in submit_candidates.values():
                if stage.name in all_paths and stage.state == StageStates.INITIALIZED:

                    if stage.context is None:
                        stage.context = SimpleContext()

                    self._update(stage)

                    stage.state = StageStates.ANALYZED

        self.source.state = StageStates.ANALYZED

        self.visited.append(self.source.name)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):

        for edge_name in self.history:

            history = self.history[edge_name]

            self.next_history[edge_name] = {}

            self.next_history[edge_name].update({
                key: value for key, value  in history.items() if key in self.provides
            })

        if self.skip_stage is False:

            self.next_history.update({
                (self.source.name, destination.name): {
                    'analyze_stage_summary_metrics': self.edge_data.get(
                        'analyze_stage_summary_metrics', 
                        {}
                    ),
                    'analyze_stage_custom_metrics_set': self.edge_data.get(
                        'analyze_stage_custom_metrics_set',
                        {}
                    )
                }
            })

    def split(self, edges: List[AnalyzeEdge]) -> None:
        submit_candidates = self.generate_submit_candidates()

        analyze_stage_config: Dict[str, Any] = self.source.to_copy_dict()

        analyze_stage_copy = type(self.source.name, (Analyze, ), {})()
        
        for copied_attribute_name, copied_attribute_value in analyze_stage_config.items():
            if inspect.ismethod(copied_attribute_value) is False:
                setattr(analyze_stage_copy, copied_attribute_name, copied_attribute_value)

        user_hooks: Dict[str, Hook] = {}
        for hooks in registrar.all.values():
            for hook in hooks:
                if hasattr(self.source, hook.shortname) and not hasattr(Analyze, hook.shortname):
                    user_hooks = {
                        hook.shortname: hook._call
                    }

        analyze_stage_copy.dispatcher = self.source.dispatcher.copy()

        edge_candidates = self._generate_edge_submit_candidates(edges)

        minimum_edge_idx = min([edge.transition_idx for edge in edges])

        assigned_candidates = [
            candidate_name for candidate_name in submit_candidates if candidate_name
        ]

        for candidate in assigned_candidates:

            if candidate in edge_candidates and self.transition_idx == minimum_edge_idx:
                self.assigned_candidates.append(candidate)

            elif candidate not in edge_candidates:
                self.assigned_candidates.append(candidate)

        analyze_stage_copy.context = SimpleContext()
        for event in analyze_stage_copy.dispatcher.events_by_name.values():
            event.context = analyze_stage_copy.context 
            event.source.stage_instance = analyze_stage_copy
            event.source.stage_instance.context = analyze_stage_copy.context
            event.source.context = analyze_stage_copy.context
            
            if event.source.shortname in user_hooks:
                hook_call = user_hooks.get(event.source.shortname)

                hook_call = hook_call.__get__(analyze_stage_copy, analyze_stage_copy.__class__)
                setattr(analyze_stage_copy, event.source.shortname, hook_call)

                event.source._call = hook_call

            else:            
                event.source._call = getattr(analyze_stage_copy, event.source.shortname)
                event.source._call = event.source._call.__get__(analyze_stage_copy, analyze_stage_copy.__class__)
                setattr(analyze_stage_copy, event.source.shortname, event.source._call)
          
        self.source = analyze_stage_copy

        if minimum_edge_idx < self.transition_idx:
            self.skip_stage = True

    def _generate_edge_submit_candidates(self, edges: List[AnalyzeEdge]):

        candidates = []

        for edge in edges:
            if edge.transition_idx != self.transition_idx:
                analyze_candidates = edge.generate_submit_candidates()
                destination_path = edge.all_paths.get(edge.destination.name)
                candidates.extend([
                    candidate_name for candidate_name in analyze_candidates if candidate_name in destination_path
                ])

        return candidates

    def generate_submit_candidates(self) -> Dict[str, Stage]:
    
        submit_stages: Dict[str, Submit] = self.stages_by_type.get(StageTypes.SUBMIT)
        analyze_stages = self.stages_by_type.get(StageTypes.ANALYZE).items()
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        all_paths = self.all_paths.get(self.source.name, [])

        analyze_stages_in_path = {}
        for stage_name, stage in analyze_stages:
            if stage_name in all_paths and stage_name != self.source.name and stage_name not in self.visited:
                analyze_stages_in_path[stage_name] = self.all_paths.get(stage_name)

        submit_candidates: Dict[str, Stage] = {}

        for stage_name, stage in submit_stages.items():
            if stage_name in all_paths:
                if len(analyze_stages_in_path) > 0:
                    for path in analyze_stages_in_path.values():
                        if stage_name not in path:
                            stage.state = StageStates.ANALYZING
                            submit_candidates[stage_name] = stage

                else:
                    submit_candidates[stage_name] = stage

        selected_submit_candidates: Dict[str, Stage] = {}
        following_opimize_stage_distances = [
            path_length for stage_name, path_length in path_lengths.items() if stage_name in analyze_stages
        ]

        for stage_name in path_lengths.keys():
            stage_distance = path_lengths.get(stage_name)

            if stage_name in submit_candidates:

                if len(following_opimize_stage_distances) > 0 and stage_distance < min(following_opimize_stage_distances):
                    selected_submit_candidates[stage_name] = submit_candidates.get(stage_name)

                elif len(following_opimize_stage_distances) == 0:
                    selected_submit_candidates[stage_name] = submit_candidates.get(stage_name)

        return selected_submit_candidates

    def setup(self) -> None:

        raw_results = {}
        for from_stage_name in self.from_stage_names:
            
            stage_results = self.history[(
                from_stage_name, 
                self.source.name
            )].get('execute_stage_results')

            if stage_results:
                raw_results.update(stage_results)

        execute_stages = self.stages_by_type.get(StageTypes.EXECUTE)

        results_to_calculate = {}
        target_stages = {}
        for stage_name in raw_results.keys():
            stage = execute_stages.get(stage_name)
            all_paths = self.all_paths.get(stage_name)

            in_path = self.source.name in all_paths

            if in_path:
                stage.state = StageStates.ANALYZING
                results_to_calculate[stage_name] = raw_results.get(stage_name)
                target_stages[stage_name] = stage
        
        
        self.edge_data['analyze_stage_raw_results'] = results_to_calculate
        self.edge_data['analyze_stage_target_stages'] = target_stages
        self.edge_data['analyze_stage_has_results'] = len(results_to_calculate) > 0