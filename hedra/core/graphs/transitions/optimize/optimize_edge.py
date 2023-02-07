import asyncio
from collections import defaultdict
from typing import Dict, Any
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.optimize.optimize import Optimize
from hedra.core.graphs.stages.execute.execute import Execute
from hedra.core.engines.types.common.results_set import ResultsSet
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


class OptimizeEdge(BaseEdge[Optimize]):

    def __init__(self, source: Optimize, destination: Stage) -> None:
        super(
            OptimizeEdge,
            self
        ).__init__(
            source,
            destination
        )

        self.history = {
            'optimize_stage_candidates': {},
            'optimize_stage_optimized_params': [],
            'optimize_stage_optimized_configs': {},
            'optimzie_stage_optimized_hooks': defaultdict(list),
            'execute_stages_setup_by': {}
        }

        self.requires = [
            'execute_stage_setup_hooks',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'setup_stage_ready_stages',
            'setup_stage_candidates',
        ]

        self.provides = [
            'optimize_stage_optimized_params',
            'optimize_stage_optimized_configs',
            'optimzie_stage_optimized_hooks',
            'execute_stage_setup_by'
        ]
        

    async def transition(self):
        history = self.history[self.from_stage_name]

        execute_stages: Dict[str, Execute] = history['setup_stage_ready_stages']
        optimize_stages = self.stages_by_type.get(StageTypes.OPTIMIZE).items()
        paths = self.all_paths.get(self.source.name)
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        execute_stages = {
            stage_name: stage for stage_name, stage in execute_stages.items() if stage_name in paths and stage_name not in self.visited
        }

        optimize_stages_in_path = {}
        for stage_name, stage in optimize_stages:
            if stage_name in paths and stage_name != self.source.name and stage_name not in self.visited:
                optimize_stages_in_path[stage_name] = self.all_paths.get(stage_name)

        optimization_candidates: Dict[str, Stage] = {}

        valid_states = [
            StageStates.INITIALIZED,
            StageStates.SETUP,
        ]

        for stage_name, stage in execute_stages.items():
            if stage_name in paths and stage.state in valid_states:

                if len(optimize_stages_in_path) > 0:
                    for path in optimize_stages_in_path.values():
                        if stage_name not in path:
                            stage.state = StageStates.OPTIMIZING
                            stage.context['execute_stage_setup_config'] = history['execute_stage_setup_config']
                            stage.context['execute_stage_setup_by'] = history['execute_stage_setup_by']
                            optimization_candidates[stage_name] = stage

                else:
                    stage.state = StageStates.OPTIMIZING
                    stage.context['execute_stage_setup_config'] = history['execute_stage_setup_config']
                    stage.context['execute_stage_setup_by'] = history['execute_stage_setup_by']
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
                    

        history['optimize_stage_candidates'] = selected_optimization_candidates

        for event in self.source.dispatcher.events_by_name.values():
            self.source.context.update(history)
            event.context.update(history)
            
            if event.source.context:
                event.source.context.update(history)

        if len(selected_optimization_candidates) > 0:
            self.source.generation_optimization_candidates = len(selected_optimization_candidates)

            if self.timeout:
                await asyncio.wait_for(self.source.run(), timeout=self.timeout)

            else:
                await self.source.run()

        for provided in self.provides:
            history[provided] = self.source.context[provided]

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

        history = self.history[self.from_stage_name]

        optimized_config: Stage = history['optimize_stage_optimized_configs'].get(destination.name)
        optimzied_hooks: Stage = history['optimzie_stage_optimized_hooks'].get(destination.name)
        stage_setup_by: str = history['execute_stage_setup_by'].get(destination.name)

        if optimized_config and optimzied_hooks and stage_setup_by:

            self.next_history[destination.name] = {
                self.source.name: {
                    'optimize_stage_optimized_params': history['optimize_stage_optimized_params'],
                    'setup_stage_candidates': list(history['optimize_stage_candidates'].keys()),
                    'setup_stage_ready_stages': history['setup_stage_ready_stages'],
                    'execute_stage_setup_config': optimized_config,
                    'execute_stage_setup_hooks': optimzied_hooks,
                    'execute_stage_setup_by': stage_setup_by   
                }
            }

