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

        history = self.history[(self.from_stage_name, self.source.name)]


        ready_stages = history.get('setup_stage_ready_stages', {})
        setup_candidates = history.get('setup_stage_candidates', {})
        setup_config = history.get('execute_stage_setup_config')
        execute_stage_setup_hooks = []
        setup_execute_stage: Execute = ready_stages.get(self.source.name)

        if setup_execute_stage:
            execute_stage_setup_hooks = setup_execute_stage.context['execute_stage_setup_hooks']
        

        self.stages_by_type[StageTypes.EXECUTE].update(ready_stages)

        self.next_history[(self.source.name, destination.name)] = {
            'execute_stage_setup_hooks': execute_stage_setup_hooks,
            'setup_stage_ready_stages': ready_stages,
            'setup_stage_candidates': list(setup_candidates.keys()),
            'execute_stage_setup_config': setup_config,
            'execute_stage_setup_by': self.source.name   
        }
        

    def split(self, edges: List[BaseEdge]) -> None:
        edges_count = len(edges)
        setup_candidates = self.get_setup_candidates()
        candidate_names = list(setup_candidates.keys())
        candidates_count = len(candidate_names)

        candidates_per_edge = math.floor(candidates_count/edges_count)

        if self.transition_idx == (edges_count - 1):
            candidates_per_edge += candidates_count%edges_count

        batch_start = candidates_per_edge * self.transition_idx
        batch_end = batch_start + candidates_per_edge + 1

        selected_candidates = candidate_names[batch_start:batch_end]
        self.assigned_candidates = selected_candidates

        discovered: Dict[str, Stage] = import_stages(self.source.graph_path)
        
        initialized_stages = {}
        hooks_by_type = defaultdict(dict)
        hooks_by_name = {}
        hooks_by_shortname = defaultdict(dict)

        generated_hooks = {}
        for stage in discovered.values():
            stage: Stage = stage()
            stage.graph_name = self.source.graph_name
            stage.graph_path = self.source.graph_path
            stage.graph_id = self.source.generation_id

            for hook_shortname, hook in registrar.reserved[stage.name].items():
                hook._call = hook._call.__get__(stage, stage.__class__)
                setattr(stage, hook_shortname, hook._call)

            initialized_stage = set_stage_hooks(
                stage, 
                generated_hooks
            )

            for hook_type in initialized_stage.hooks:

                for hook in initialized_stage.hooks[hook_type]:
                    hooks_by_type[hook_type][hook.name] = hook
                    hooks_by_name[hook.name] = hook
                    hooks_by_shortname[hook_type][hook.shortname] = hook

            initialized_stages[initialized_stage.name] = initialized_stage

        setup_stage: Setup = initialized_stages.get(self.source.name)
        setup_stage.context.update(self.source.context)
        setup_stage.plugins = self.source.plugins

        for hook_type in setup_stage.hooks:
            for hook in setup_stage.hooks[hook_type]:
                hooks_by_type[hook_type][hook.name] = hook

        events_graph = EventGraph(hooks_by_type)
        events_graph.hooks_by_name = hooks_by_name
        events_graph.hooks_by_shortname = hooks_by_shortname
        events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()

        for stage in initialized_stages.values():
            stage.dispatcher.assemble_action_and_task_subgraphs()

        self.source = setup_stage

    def get_setup_candidates(self) -> Dict[str, Execute]:
        execute_stages = [(stage_name, stage) for stage_name, stage in self.stages_by_type.get(StageTypes.EXECUTE).items()]

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

        return setup_candidates