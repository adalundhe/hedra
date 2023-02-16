import asyncio
import math
from collections import defaultdict
from typing import Dict, List
from hedra.core.graphs.transitions.common.base_edge import BaseEdge
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.simple_context import SimpleContext
from hedra.core.graphs.stages.types.stage_states import StageStates
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.events.event_graph import EventGraph
from hedra.core.graphs.stages.base.import_tools import (
    import_stages, 
    import_plugins, 
    set_stage_hooks
)

from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.engines.types.registry import registered_engines
from hedra.core.personas.persona_registry import registered_personas
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin

from typing import TypeVar

Cls = TypeVar('Cls')


def copy_class(cls: Cls) -> Cls:
    copy_cls = type(f'{cls.__class__.__name__}', cls.__class__.__bases__, dict(cls.__dict__))()
    for name, attr in cls.__dict__.items():
        if not name.startswith('__'):
            setattr(copy_cls, name, attr)
    return copy_cls


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

        self.requires = []
        self.provides = [
            'execute_stage_setup_hooks',
            'execute_stage_setup_config',
            'execute_stage_setup_by',
            'setup_stage_ready_stages',
            'setup_stage_candidates',
        ]

        self.assigned_candidates = []

    async def transition(self):
        self.source.state = StageStates.SETTING_UP

        history = self.history[(self.from_stage_name, self.source.name)]

        setup_candidates = self.get_setup_candidates()

        if len(self.assigned_candidates) > 0:
            setup_candidates = {
                stage_name: stage for stage_name, stage in setup_candidates.items() if stage_name in self.assigned_candidates
            }

        self.source.generation_setup_candidates = len(setup_candidates)

        history['setup_stage_target_stages'] = setup_candidates
        history['setup_stage_target_config'] = self.source.config

        self.source.context.update(history)

        for event in self.source.dispatcher:

            event.context.update(history)
            
            if event.source.context:
                event.source.context.update(history)
        
        if self.timeout:
            await asyncio.wait_for(self.source.run(), timeout=self.timeout)
        else:
            await self.source.run()

        for provided in self.provides:
            history[provided] = self.source.context[provided]

        history['setup_stage_candidates'] = setup_candidates

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

        for edge_name in self.history:

            history = self.history[edge_name]

            if self.next_history.get(edge_name) is None:
                self.next_history[edge_name] = {}

            self.next_history[edge_name].update({
                key: value for key, value  in history.items() if key in self.provides
            })

        history = self.history[(self.from_stage_name, self.source.name)]

        ready_stages = history.get('setup_stage_ready_stages', {})
        setup_candidates = history.get('setup_stage_candidates', {})
        setup_config = history.get('execute_stage_setup_config')
        execute_stage_setup_hooks = []
        setup_execute_stage: Execute = ready_stages.get(self.source.name)

        if setup_execute_stage:
            execute_stage_setup_hooks = setup_execute_stage.context['execute_stage_setup_hooks']

        self.stages_by_type[StageTypes.EXECUTE].update(ready_stages)

        if self.next_history.get((self.source.name, destination.name)) is None:
            self.next_history[(self.source.name, destination.name)] = {}

        self.next_history[(self.source.name, destination.name)].update({
            'execute_stage_setup_hooks': execute_stage_setup_hooks,
            'setup_stage_ready_stages': ready_stages,
            'setup_stage_candidates': list(setup_candidates.keys()),
            'execute_stage_setup_config': setup_config,
            'execute_stage_setup_by': self.source.name   
        })
        

    def split(self, edges: List[BaseEdge]) -> None:
        pass

    def get_setup_candidates(self) -> Dict[str, Execute]:
        execute_stages = [(stage_name, stage) for stage_name, stage in self.stages_by_type.get(StageTypes.EXECUTE).items()]

        all_paths = self.all_paths.get(self.source.name, [])

        path_lengths: Dict[str, int] = self.path_lengths.get(self.source.name)

        execute_stages: Dict[str, Execute] = {
            stage_name: stage for stage_name, stage in execute_stages if stage_name in all_paths and stage_name not in self.visited
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

        return setup_candidates
