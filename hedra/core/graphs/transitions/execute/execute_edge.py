import asyncio
from typing import Dict
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.execute.execute import Execute
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes


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
            'setup_stage_ready_stages'
        ]

        self.valid_states = [
            StageStates.SETUP,
            StageStates.OPTIMIZED
        ]

    async def transition(self):

        self.source.state = StageStates.EXECUTING

        history = self.history[(self.from_stage_name, self.source.name)]

        execute_stages = self.stages_by_type.get(StageTypes.EXECUTE)
        analyze_stages: Dict[str, Stage] = self.stages_by_type.get(StageTypes.ANALYZE)

        total_concurrent_execute_stages = []
        for generation_stage_name in self.source.generation_stage_names:
            stage = execute_stages.get(generation_stage_name)
            
            if stage is not None:
                total_concurrent_execute_stages.append(stage)

        self.source.total_concurrent_execute_stages = len(total_concurrent_execute_stages)

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
            self.history[(self.from_stage_name, self.source.name)][provided] = self.source.context[provided]
        
        if self.destination.context is None:
            self.destination.context = SimpleContext()

        self._update(self.destination)

        all_paths = self.all_paths.get(self.source.name, [])

        for stage in analyze_stages.values():
            if stage.name in all_paths and stage.state == StageStates.INITIALIZED:
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

        history = self.history[(self.from_stage_name, self.source.name)]

        next_results = self.next_history.get((self.source.name, destination.name))
        if next_results is None:
            next_results = {}

        next_results.update({
            'execute_stage_results': {
                self.source.name: history['execute_stage_results']
            },
            'execute_stage_setup_config': history['execute_stage_setup_config'],
            'execute_stage_setup_hooks': history['execute_stage_setup_hooks'],
            'execute_stage_setup_by': history['execute_stage_setup_by'],
            'setup_stage_ready_stages': history['setup_stage_ready_stages'],
            'setup_stage_candidates': history['setup_stage_candidates']
        })

        self.next_history.update({
            (self.source.name, destination.name): next_results
        })

    def split(self) -> None:
        pass