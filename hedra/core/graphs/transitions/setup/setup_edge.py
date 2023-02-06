class SetupEdge:

    def __init__(self) -> None:
        self.wants = []
        self.provides = []

        self.values = {}
        self.paths = []

    async def connect(self):
        pass


import asyncio
from collections import defaultdict
from typing import Dict, Any
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.simple_context import SimpleContext
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

        self.history = {
            'setup_stage_target_stages': [],
            'setup_stage_target_config': None,
            'setup_stage_ready_stages': {},
            'execute_stage_setup_by': None,
            'execute_stage_setup_hooks': {}
        }

        self.valid_states = [
            StageStates.INITIALIZED,
            StageStates.VALIDATED
        ]

        self.requires = []
        self.provides = [
            'execute_stage_setup_hooks',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'setup_stage_ready_stages',
            'setup_stage_candidates',
        ]

    async def transition(self):
        self.source.state = StageStates.SETTING_UP

        execute_stages = self.stages_by_type.get(StageTypes.EXECUTE).items()
        paths = self.all_paths.get(self.source.name)
        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        execute_stages: Dict[str, Execute] = {
            stage_name: stage for stage_name, stage in execute_stages if stage_name in paths and stage_name not in self.visited
        }

        setup_candidates: Dict[str, Execute] = {}
        
        setup_stages: Dict[str, Setup] = self.stages_by_type.get(StageTypes.SETUP)
        execute_stages: Dict[str, Execute] = self.stages_by_type.get(StageTypes.EXECUTE)
        following_setup_stage_distances = [
            path_length for stage_name, path_length in path_lengths.items() if stage_name in setup_stages
        ]

        for stage_name in path_lengths.keys():
            stage_distance = path_lengths.get(stage_name)

            if stage_name in execute_stages:

                if len(following_setup_stage_distances) > 0 and stage_distance < min(following_setup_stage_distances):
                    setup_candidates[stage_name] = execute_stages.get(stage_name)

                elif len(following_setup_stage_distances) == 0:
                    setup_candidates[stage_name] = execute_stages.get(stage_name)

        self.source.generation_setup_candidates = len(setup_candidates)

        self.history['setup_stage_target_stages'] = setup_candidates
        self.history['setup_stage_target_config'] = self.source.config

        for event in self.source.dispatcher.events_by_name.values():
            event.context.update(self.history)
            
            if event.source.context:
                event.source.context.update(self.history)
        
        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        else:
            await self.source.run()

        for provided in self.provides:
            self.history[provided] = self.source.context[provided]

        self.history['setup_stage_candidates'] = setup_candidates

        self._update(self.destination)

        if self.destination.context is None:
            self.destination.context = SimpleContext()

        for execute_stage in setup_candidates.values():
            execute_stage.state = StageStates.SETUP

            if execute_stage.context is None:
                execute_stage.context = SimpleContext()

            self._update(execute_stage)
        
        self.visited.append(self.source.name)

        return None, self.destination.stage_type

    def _update(self, destination: Stage):

        ready_stages = self.history.get('setup_stage_ready_stages', {})
        setup_candidates = self.history.get('setup_stage_candidates', {})
        setup_config = self.history.get('execute_stage_setup_config')
        

        self.stages_by_type[StageTypes.EXECUTE].update(ready_stages)

        setup_execute_stage: Execute = ready_stages.get(self.source.name)

        self.next_history[destination.name] = {
            'setup_stage_ready_stages': ready_stages,
            'setup_stage_candidates': [],
            'execute_stage_setup_config': None,
            'execute_stage_setup_by': None   
        }

        if setup_execute_stage:
            self.next_history[destination.name]['execute_stage_setup_hooks'] = setup_execute_stage.context['execute_stage_setup_hooks']
        
        self.next_history[destination.name]['setup_stage_candidates'].extend(list(setup_candidates.keys()))
        self.next_history[destination.name]['execute_stage_setup_config'] = setup_config
        self.next_history[destination.name]['execute_stage_setup_by'] = self.source.name