from __future__ import annotations
import asyncio
import inspect
import traceback
from collections import defaultdict
from typing import Dict, List, Any, Union
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.hooks.types.base.event_types import EventType
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.graphs.stages.execute import Execute
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


class SetupEdge(BaseEdge[Setup]):

    def __init__(self, source: Setup, destination: BaseEdge[Stage]) -> None:
        super(
            SetupEdge,
            self
        ).__init__(
            source,
            destination
        )

        self.valid_states = [
            StageStates.INITIALIZED,
            StageStates.VALIDATED
        ]

        self.requires = [
            'execute_stage_streamed_analytics',
            'execute_stage_results'
        ]

        self.provides = [
            'setup_stage_experiment_config',
            'execute_stage_streamed_analytics',
            'execute_stage_setup_hooks',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'setup_stage_ready_stages',
            'setup_stage_candidates',
            'setup_stage_configs',
            'execute_stage_results'
        ]

        self.assigned_candidates = []
        self.assigned_optimize_candidates = []

    async def transition(self):
        
        try:
            self.source.state = StageStates.SETTING_UP

            setup_candidates = self.get_setup_candidates()
            optimize_candidates = self.get_optimize_candidates()

            self.source.generation_setup_candidates = len(setup_candidates)

            for setup_candidate in setup_candidates.values():
                if setup_candidate.context is None:
                    setup_candidate.context = SimpleContext()

            
            for optimize_candidate in optimize_candidates.values():
                if optimize_candidate.context is None:
                    optimize_candidate.context = SimpleContext()

            self.edge_data['setup_stage_target_stages'] = setup_candidates
            self.edge_data['setup_stage_target_config'] = self.source.config

            self.source.context.update(self.edge_data)

            for event in self.source.dispatcher.events_by_name.values():
                event.source.stage_instance = self.source
                self.source.context.update(self.edge_data)
                event.context.update(self.edge_data)
                
                if event.source.context:
                    event.source.context.update(self.edge_data)
            
            if self.timeout and self.skip_stage is False:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)
            elif self.skip_stage is False:
                await self.source.run()

            for provided in self.provides:
                self.edge_data[provided] = self.source.context[provided]

            self.edge_data['setup_stage_candidates'] = setup_candidates

            self._update(self.destination)

            if self.destination.context is None:
                self.destination.context = SimpleContext()

            for execute_stage in setup_candidates.values():

                if execute_stage.name != self.destination.name:
                    execute_stage.state = StageStates.SETUP

                    if execute_stage.context is None:
                        execute_stage.context = SimpleContext()
                    
                    self._update(execute_stage)

            
            for optimize_stage in optimize_candidates.values():

                if optimize_stage.name != self.destination.name:
                    optimize_stage.state = StageStates.SETUP

                    if optimize_stage.context is None:
                        optimize_stage.context = SimpleContext()

                    self._update(optimize_stage)
            
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


        if self.next_history.get((self.source.name, destination.name)) is None:
            self.next_history[(self.source.name, destination.name)] = {}

        if self.skip_stage is False:
            setup_stage_configs = self.edge_data.get('setup_stage_configs', {})
            ready_stages = self.edge_data.get('setup_stage_ready_stages', {})
            setup_candidates = self.edge_data.get('setup_stage_candidates', {})
            setup_config = self.edge_data.get('execute_stage_setup_config')
            setup_stage_experiment_config: Dict[str, Union[str, int, List[float]]] = self.edge_data.get('setup_stage_experiment_config', {})
            execute_stage_setup_hooks = []
            setup_execute_stage: Execute = ready_stages.get(self.source.name)

            if setup_execute_stage:
                execute_stage_setup_hooks = setup_execute_stage.context['execute_stage_setup_hooks']

            self.stages_by_type[StageTypes.EXECUTE].update(ready_stages)

            self.next_history[(self.source.name, destination.name)].update({
                'setup_stage_experiment_config': setup_stage_experiment_config,
                'setup_stage_configs': setup_stage_configs,
                'execute_stage_setup_hooks': execute_stage_setup_hooks,
                'setup_stage_ready_stages': ready_stages,
                'setup_stage_candidates': list(setup_candidates.keys()),
                'execute_stage_setup_config': setup_config,
                'execute_stage_setup_by': self.source.name   
            })
            

    def split(self, edges: List[SetupEdge]) -> None:

        setup_stage_config: Dict[str, Any] = self.source.to_copy_dict()

        setup_stage_copy = type(self.source.name, (Setup, ), {})()
        
        for copied_attribute_name, copied_attribute_value in setup_stage_config.items():
            if inspect.ismethod(copied_attribute_value) is False:
                setattr(setup_stage_copy, copied_attribute_name, copied_attribute_value)

        user_hooks: Dict[str, Dict[str, Hook]] = defaultdict(dict)
        for hooks in registrar.all.values():
            for hook in hooks:
                if hasattr(self.source, hook.shortname) and not hasattr(Execute, hook.shortname):
                    user_hooks[self.source.name][hook.shortname] = hook._call

        setup_stage_copy.dispatcher = self.source.dispatcher.copy()

        minimum_edge_idx = min([edge.transition_idx for edge in edges])

        setup_stage_copy.context = SimpleContext()
        for event in setup_stage_copy.dispatcher.events_by_name.values():
            event.context = setup_stage_copy.context 
            event.source.stage_instance = setup_stage_copy
            event.source.stage_instance.context = setup_stage_copy.context
            event.source.context = setup_stage_copy.context

            if event.source.shortname in user_hooks[setup_stage_copy.name]:
                hook_call = user_hooks[setup_stage_copy.name].get(event.source.shortname)

                hook_call = hook_call.__get__(setup_stage_copy, setup_stage_copy.__class__)
                setattr(setup_stage_copy, event.source.shortname, hook_call)

                event.source._call = hook_call

            else:            
                event.source._call = getattr(setup_stage_copy, event.source.shortname)
                event.source._call = event.source._call.__get__(setup_stage_copy, setup_stage_copy.__class__)
                setattr(setup_stage_copy, event.source.shortname, event.source._call)
          
        self.source = setup_stage_copy

        if minimum_edge_idx < self.transition_idx:
            self.skip_stage = True

    def get_setup_candidates(self) -> Dict[str, Execute]:
        execute_stages: Dict[str, Execute] = self.stages_by_type.get(StageTypes.EXECUTE)
        setup_stages: Dict[str, Setup] = self.stages_by_type.get(StageTypes.SETUP)
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        all_paths = self.all_paths.get(self.source.name, [])

        execute_candidates: Dict[str, Stage] = {}

        for stage_name, stage in execute_stages.items():
            if stage_name in all_paths:
                execute_candidates[stage_name] = stage    

        selected_execute_candidates: Dict[str, Execute] = {}
        
        following_setup_stage_distances = [
            path_length for stage_name, path_length in path_lengths.items() if stage_name in setup_stages
        ]

        for stage_name in path_lengths.keys():
            stage_distance = path_lengths.get(stage_name)

            if stage_name in execute_candidates:

                if len(following_setup_stage_distances) > 0 and stage_distance <= min(following_setup_stage_distances):
                    selected_execute_candidates[stage_name] = execute_candidates.get(stage_name)

                elif len(following_setup_stage_distances) == 0:
                    selected_execute_candidates[stage_name] = execute_candidates.get(stage_name)

        for candidate in selected_execute_candidates.values():
            actions = [event for event in candidate.dispatcher.actions_and_tasks.values() if event.event_type == EventType.ACTION]
            tasks = [event for event in candidate.dispatcher.actions_and_tasks.values() if event.event_type == EventType.TASK]

            candidate.hooks[HookType.ACTION] = actions
            candidate.hooks[HookType.TASK] = tasks

        return selected_execute_candidates
    
    def get_optimize_candidates(self) -> Dict[str, Execute]:
        optimize_stages: Dict[str, Execute] = self.stages_by_type.get(StageTypes.OPTIMIZE, {})
        setup_stages: Dict[str, Setup] = self.stages_by_type.get(StageTypes.SETUP)
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        all_paths = self.all_paths.get(self.source.name, [])

        optimize_candidates: Dict[str, Stage] = {}

        for stage_name, stage in optimize_stages.items():
            if stage_name in all_paths:
                optimize_candidates[stage_name] = stage    

        selected_optimize_candidates: Dict[str, Execute] = {}
        
        following_setup_stage_distances = [
            path_length for stage_name, path_length in path_lengths.items() if stage_name in setup_stages or stage_name in optimize_stages
        ]

        for stage_name in path_lengths.keys():
            stage_distance = path_lengths.get(stage_name)

            if stage_name in optimize_candidates:

                if len(following_setup_stage_distances) > 0 and stage_distance <= min(following_setup_stage_distances):
                    selected_optimize_candidates[stage_name] = optimize_candidates.get(stage_name)

                elif len(following_setup_stage_distances) == 0:
                    selected_optimize_candidates[stage_name] = optimize_candidates.get(stage_name)

        return selected_optimize_candidates
