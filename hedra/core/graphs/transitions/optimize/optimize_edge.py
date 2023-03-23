from __future__ import annotations
import asyncio
import inspect
from typing import Dict, List, Any, Union
from hedra.core.engines.client.config import Config
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.optimize.optimize import Optimize
from hedra.core.graphs.stages.execute.execute import Execute
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


ExecuteHooks = List[Union[ActionHook , TaskHook]]


class OptimizeEdge(BaseEdge[Optimize]):

    def __init__(self, source: Optimize, destination: Stage) -> None:
        super(
            OptimizeEdge,
            self
        ).__init__(
            source,
            destination
        )

        self.requires = [
            'execute_stage_setup_hooks',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'setup_stage_ready_stages',
            'setup_stage_candidates',
            'execute_stage_results'
        ]

        self.provides = [
            'optimize_stage_optimized_params',
            'optimize_stage_optimized_configs',
            'optimize_stage_optimized_hooks',
            'execute_stage_setup_by',
            'execute_stage_results'
        ]

        self.assigned_candidates = []
        

    async def transition(self):

        selected_optimization_candidates = self.generate_optimization_candidates()

        if len(self.assigned_candidates) > 0:
            selected_optimization_candidates = {
                stage_name: stage for stage_name, stage in selected_optimization_candidates.items() if stage_name in self.assigned_candidates
            }

        self.edge_data['optimize_stage_candidates'] = selected_optimization_candidates
        
        self.source.context.update(self.edge_data)
        
        for event in self.source.dispatcher.events_by_name.values():
            event.source.stage_instance = self.source
            event.context.update(self.edge_data)
            
            if event.source.context:
                event.source.context.update(self.edge_data)

        if len(selected_optimization_candidates) > 0:
            self.source.generation_optimization_candidates = len(selected_optimization_candidates)

            if self.timeout and self.skip_stage is False:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)

            elif self.skip_stage is False:
                await self.source.run()

        for provided in self.provides:
            self.edge_data[provided] = self.source.context[provided]

        if self.destination.context is None:
            self.destination.context = SimpleContext()
        
        self._update(self.destination)
        if len(selected_optimization_candidates) > 0:

            if self.destination.context is None:
                self.destination.context = SimpleContext()

            for optimization_candidate in selected_optimization_candidates.values():

                if optimization_candidate.context is None:
                    optimization_candidate.context = SimpleContext()

                self._update(optimization_candidate)

                optimization_candidate.state = StageStates.OPTIMIZED

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



        if self.next_history.get((self.source.name, destination.name)) is None:
            self.next_history[(self.source.name, destination.name)] = {}

        if self.skip_stage:

            stage_setup_by: str = self.edge_data['execute_stage_setup_by'].get(destination.name)

            self.next_history[(self.source.name, destination.name)].update({
                'optimize_stage_optimized_params': {},
                'setup_stage_candidates': [],
                'setup_stage_ready_stages': self.edge_data['setup_stage_ready_stages'],
                'execute_stage_setup_config': None,
                'execute_stage_setup_hooks': None,
                'execute_stage_setup_by': stage_setup_by 
            })

        else:

            optimized_config: Stage = self.edge_data['optimize_stage_optimized_configs'].get(destination.name)
            optimzied_hooks: Stage = self.edge_data['optimize_stage_optimized_hooks'].get(destination.name)
            stage_setup_by: str = self.edge_data['execute_stage_setup_by'].get(destination.name)

            self.next_history[(self.source.name, destination.name)].update({
                'optimize_stage_optimized_params': self.edge_data['optimize_stage_optimized_params'],
                'setup_stage_candidates': list(self.edge_data['optimize_stage_candidates'].keys()),
                'setup_stage_ready_stages': self.edge_data['setup_stage_ready_stages'],
                'execute_stage_setup_config': optimized_config,
                'execute_stage_setup_hooks': optimzied_hooks,
                'execute_stage_setup_by': stage_setup_by 
            })

            

    def split(self, edges: List[OptimizeEdge]) -> None:
        optimze_candidates = self.generate_optimization_candidates()

        optimize_stage_config: Dict[str, Any] = self.source.to_copy_dict()

        optimize_stage_copy: Optimize = type(self.source.name, (Optimize, ), self.source.__dict__)()
        
        for copied_attribute_name, copied_attribute_value in optimize_stage_config.items():
            if inspect.ismethod(copied_attribute_value) is False:
                setattr(optimize_stage_copy, copied_attribute_name, copied_attribute_value)
        
        optimize_stage_copy.dispatcher = self.source.dispatcher.copy()

        for event in optimize_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = optimize_stage_copy

        edge_candidates = self._generate_edge_optimize_candidates(edges)

        destination_path = self.all_paths.get(self.destination.name)

        minimum_edge_idx = min([edge.transition_idx for edge in edges])

        assigned_candidates = [
            candidate_name for candidate_name in optimze_candidates if candidate_name in destination_path
        ]

        for candidate in assigned_candidates:

            if candidate in edge_candidates and self.transition_idx == minimum_edge_idx:
                self.assigned_candidates.append(candidate)

            elif candidate not in edge_candidates:
                self.assigned_candidates.append(candidate)

        
        optimize_stage_copy.context = SimpleContext()
        for event in optimize_stage_copy.dispatcher.events_by_name.values():
            event.source.stage_instance = optimize_stage_copy 
            event.source.stage_instance = optimize_stage_copy
            event.source.stage_instance.context = optimize_stage_copy.context
            event.source.context = optimize_stage_copy.context

            event.source._call = getattr(optimize_stage_copy, event.source.shortname)
            
            event.source._call = event.source._call.__get__(optimize_stage_copy, optimize_stage_copy.__class__)
            setattr(optimize_stage_copy, event.source.shortname, event.source._call)

        self.source = optimize_stage_copy

    def _generate_edge_optimize_candidates(self, edges: List[OptimizeEdge]):

        candidates = []

        for edge in edges:
            if edge.transition_idx != self.transition_idx:
                optimize_candidates = edge.generate_optimization_candidates()
                destination_path = edge.all_paths.get(edge.destination.name)
                candidates.extend([
                    candidate_name for candidate_name in optimize_candidates if candidate_name in destination_path
                ])

        return candidates

    def generate_optimization_candidates(self) -> Dict[str, Stage]:
    
        execute_stages: Dict[str, Execute] = self.stages_by_type.get(StageTypes.EXECUTE)
        optimize_stages = self.stages_by_type.get(StageTypes.OPTIMIZE).items()
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        all_paths = self.all_paths.get(self.source.name, [])

        optimize_stages_in_path = {}
        for stage_name, stage in optimize_stages:
            if stage_name in all_paths and stage_name != self.source.name and stage_name not in self.visited:
                optimize_stages_in_path[stage_name] = self.all_paths.get(stage_name)

        optimization_candidates: Dict[str, Stage] = {}

        for stage_name, stage in execute_stages.items():
            if stage_name in all_paths:
                if len(optimize_stages_in_path) > 0:
                    for path in optimize_stages_in_path.values():
                        if stage_name not in path:
                            stage.state = StageStates.OPTIMIZING
                            stage.context['execute_stage_setup_config'] = self.edge_data['execute_stage_setup_config']
                            stage.context['execute_stage_setup_by'] = self.edge_data['execute_stage_setup_by']
                            optimization_candidates[stage_name] = stage

                else:
                    stage.context['execute_stage_setup_config'] = self.edge_data['execute_stage_setup_config']
                    stage.context['execute_stage_setup_by'] = self.edge_data['execute_stage_setup_by']
                    optimization_candidates[stage_name] = stage

        selected_optimization_candidates: Dict[str, Stage] = {}
        following_opimize_stage_distances = [
            path_length for stage_name, path_length in path_lengths.items() if stage_name in optimize_stages
        ]

        for stage_name in path_lengths.keys():
            stage_distance = path_lengths.get(stage_name)

            if stage_name in optimization_candidates:

                if len(following_opimize_stage_distances) > 0 and stage_distance < min(following_opimize_stage_distances):
                    selected_optimization_candidates[stage_name] = optimization_candidates.get(stage_name)

                elif len(following_opimize_stage_distances) == 0:
                    selected_optimization_candidates[stage_name] = optimization_candidates.get(stage_name)

        return selected_optimization_candidates
    
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
