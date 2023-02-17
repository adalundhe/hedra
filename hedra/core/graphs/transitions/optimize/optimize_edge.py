import asyncio
from typing import Dict, List
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
            'optimzie_stage_optimized_hooks',
            'execute_stage_setup_by',
            'execute_stage_results'
        ]
        

    async def transition(self):
        history = self.history[(self.from_stage_name, self.source.name)]

        selected_optimization_candidates = self.generate_optimization_candidates()
                    
        history['optimize_stage_candidates'] = selected_optimization_candidates
        self.source.context.update(history)

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

        for edge_name in self.history:

            history = self.history[edge_name]

            if self.next_history.get(edge_name) is None:
                self.next_history[edge_name] = {}

            self.next_history[edge_name].update({
                key: value for key, value  in history.items() if key in self.provides
            })

        history = self.history[(self.from_stage_name, self.source.name)]

        optimized_config: Stage = history['optimize_stage_optimized_configs'].get(destination.name)
        optimzied_hooks: Stage = history['optimzie_stage_optimized_hooks'].get(destination.name)
        stage_setup_by: str = history['execute_stage_setup_by'].get(destination.name)

        if self.next_history.get((self.source.name, destination.name)) is None:
            self.next_history[(self.source.name, destination.name)] = {}

        if optimized_config and optimzied_hooks and stage_setup_by:

            self.next_history[(self.source.name, destination.name)].update({
                'optimize_stage_optimized_params': history['optimize_stage_optimized_params'],
                'setup_stage_candidates': list(history['optimize_stage_candidates'].keys()),
                'setup_stage_ready_stages': history['setup_stage_ready_stages'],
                'execute_stage_setup_config': optimized_config,
                'execute_stage_setup_hooks': optimzied_hooks,
                'execute_stage_setup_by': stage_setup_by 
            })

    def split(self, edges: List[BaseEdge]) -> None:
       pass

    def generate_optimization_candidates(self) -> Dict[str, Stage]:

        history = self.history[(self.from_stage_name, self.source.name)]
        execute_stages: Dict[str, Execute] = history['setup_stage_ready_stages']
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
                            stage.context['execute_stage_setup_config'] = history['execute_stage_setup_config']
                            stage.context['execute_stage_setup_by'] = history['execute_stage_setup_by']
                            optimization_candidates[stage_name] = stage

                else:
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

        return selected_optimization_candidates
