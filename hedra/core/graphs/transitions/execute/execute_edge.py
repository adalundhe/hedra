from __future__ import annotations
import asyncio
import inspect
from typing import Dict, List, Any, Union
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.execute.execute import Execute
from hedra.core.graphs.stages.analyze.analyze import Analyze
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


ExecuteHooks = List[Union[ActionHook , TaskHook]]


class ExecuteEdge(BaseEdge[Execute]):

    def __init__(self, source: Execute, destination: BaseEdge[Stage]) -> None:
        super(
            ExecuteEdge,
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
            'execute_stage_results',
            'execute_stage_setup_hooks',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'setup_stage_ready_stages',
            'execute_stage_skipped'
        ]

        self.valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

        self.assigned_candidates = []

    async def transition(self):

        self.source.state = StageStates.EXECUTING

        execute_stages = self.stages_by_type.get(StageTypes.EXECUTE)
        analyze_stages: Dict[str, Stage] = self.generate_analyze_candidates()

        if len(self.assigned_candidates) > 0:
            analyze_stages = {
                stage_name: stage for stage_name, stage in analyze_stages.items() if stage_name in self.assigned_candidates
            }

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
        
        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self._update(self.destination)

        all_paths = self.all_paths.get(self.source.name, [])

        for stage in analyze_stages.values():
            if stage.name in all_paths:
                if stage.context is None:
                    stage.context = SimpleContext()

                self._update(stage)

        if self.destination.stage_type == StageTypes.SETUP:
   
            execute_stages = list(self.stages_by_type.get(StageTypes.EXECUTE).values())

            for stage in execute_stages:
                if stage.name not in self.visited and stage.state == StageStates.SETUP:
                    stage.state = StageStates.INITIALIZED

        self.source.state = StageStates.EXECUTED

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

        next_results = self.next_history.get((self.source.name, destination.name))
        if next_results is None:
            next_results = {}

        if self.skip_stage is False:

            next_results.update({
                'execute_stage_results': {
                    self.source.name: self.edge_data['execute_stage_results']
                },
                'execute_stage_setup_config': self.edge_data['execute_stage_setup_config'],
                'execute_stage_setup_hooks': self.edge_data['execute_stage_setup_hooks'],
                'execute_stage_setup_by': self.edge_data['execute_stage_setup_by'],
                'setup_stage_ready_stages': self.edge_data['setup_stage_ready_stages'],
                'setup_stage_candidates': self.edge_data['setup_stage_candidates']
            })

            self.next_history.update({
                (self.source.name, destination.name): next_results
            })

    def split(self, edges: List[ExecuteEdge]) -> None:

        analyze_candidates = self.generate_analyze_candidates()
        analyze_stage_config: Dict[str, Any] = self.source.to_copy_dict()

        execute_stage_copy: Execute = type(self.source.name, (Execute, ), self.source.__dict__)()
        
        for copied_attribute_name, copied_attribute_value in analyze_stage_config.items():
            if inspect.ismethod(copied_attribute_value) is False:
                setattr(
                    execute_stage_copy, 
                    copied_attribute_name, 
                    copied_attribute_value
                )

        user_hooks: Dict[str, Hook] = {}
        for hooks in registrar.all.values():
            for hook in hooks:
                if hasattr(self.source, hook.shortname) and not hasattr(Execute, hook.shortname):
                    user_hooks = {
                        hook.shortname: hook._call
                    }
        
        execute_stage_copy.dispatcher = self.source.dispatcher.copy()

        for event in execute_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = execute_stage_copy

        edge_candidates = self._generate_edge_analyze_candidates(edges)

        minimum_edge_idx = min([edge.transition_idx for edge in edges])

        assigned_candidates = [
            candidate_name for candidate_name in analyze_candidates
        ]

        for candidate in assigned_candidates:

            if candidate in edge_candidates and self.transition_idx == minimum_edge_idx:
                self.assigned_candidates.append(candidate)

            elif candidate not in edge_candidates:
                self.assigned_candidates.append(candidate)

        execute_stage_copy.context = SimpleContext()
        for event in execute_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = execute_stage_copy 
            event.source.stage_instance.context = execute_stage_copy.context
            event.source.context = execute_stage_copy.context

            if event.source.shortname in user_hooks:
                hook_call = user_hooks.get(event.source.shortname)

                hook_call = hook_call.__get__(execute_stage_copy, execute_stage_copy.__class__)
                setattr(execute_stage_copy, event.source.shortname, hook_call)

                event.source._call = hook_call

            else:            
                event.source._call = getattr(execute_stage_copy, event.source.shortname)
                event.source._call = event.source._call.__get__(execute_stage_copy, execute_stage_copy.__class__)
                setattr(execute_stage_copy, event.source.shortname, event.source._call)
 
        self.source = execute_stage_copy

        if minimum_edge_idx < self.transition_idx:
            self.skip_stage = True

    def _generate_edge_analyze_candidates(self, edges: List[ExecuteEdge]):

        candidates = []

        for edge in edges:
            if edge.transition_idx != self.transition_idx:
                analyze_candidates = edge.generate_analyze_candidates()
                destination_path = edge.all_paths.get(edge.destination.name)
                candidates.extend([
                    candidate_name for candidate_name in analyze_candidates if candidate_name in destination_path
                ])

        return candidates

    def generate_analyze_candidates(self) -> Dict[str, Stage]:
    
        submit_stages: Dict[str, Analyze] = self.stages_by_type.get(StageTypes.ANALYZE)
        analyze_stages = self.stages_by_type.get(StageTypes.EXECUTE).items()
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

        max_batch_size = 0
        execute_stage_setup_config: Config = None
        execute_stage_setup_hooks: Dict[str, ExecuteHooks] = {}
        execute_stage_setup_by: str = None
        setup_stage_ready_stages: List[Stage] = []
        setup_stage_candidates: List[Stage] = []

        for from_stage_name in self.from_stage_names:
            previous_history = self.history[(from_stage_name, self.source.name)]

            execute_config: Config = previous_history['execute_stage_setup_config']
            setup_by = previous_history['execute_stage_setup_by']

            if execute_config.optimized:
                execute_stage_setup_config = execute_config
                max_batch_size = execute_config.batch_size
                execute_stage_setup_by = setup_by

            elif execute_config.batch_size > max_batch_size:
                execute_stage_setup_config = execute_config
                max_batch_size = execute_config.batch_size
                execute_stage_setup_by = setup_by

            execute_hooks: ExecuteHooks = previous_history['execute_stage_setup_hooks']
            for setup_hook in execute_hooks:
                execute_stage_setup_hooks[setup_hook.name] = setup_hook

            ready_stages = previous_history['setup_stage_ready_stages']

            for ready_stage in ready_stages:
                if ready_stage not in setup_stage_ready_stages:
                    setup_stage_ready_stages.append(ready_stage)

            stage_candidates: List[Stage] = previous_history['setup_stage_candidates']
            for stage_candidate in stage_candidates:
                if stage_candidate not in setup_stage_candidates:
                    setup_stage_candidates.append(stage_candidate)
            

        self.edge_data = {
            'execute_stage_setup_config': execute_stage_setup_config,
            'execute_stage_setup_hooks': execute_stage_setup_hooks,
            'execute_stage_setup_by': execute_stage_setup_by,
            'setup_stage_ready_stages': setup_stage_ready_stages,
            'setup_stage_candidates': setup_stage_candidates

        }