import asyncio
import networkx
import inspect
import threading
import os
from typing import List, Dict, Union, Any, Tuple, Coroutine
from collections import defaultdict
from hedra.core.graphs.events import Event
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.error import Error
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.hooks.registry.registry_types import EventHook
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook, HookType
from hedra.core.graphs.stages.base.parallel.batch_executor import BatchExecutor
from hedra.core.graphs.transitions.exceptions.exceptions import IsolatedStageError
from hedra.logging import HedraLogger
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.reporter.reporter_plugin import ReporterPlugin
from hedra.plugins.types.plugin_types import PluginType
from .transition import Transition
from .common import (
    invalid_transition
)
from .idle import (
    invalid_idle_transition
)


class TransitionAssembler:

    def __init__(
        self, 
        transition_types, 
        graph_name: str=None,
        graph_id: str=None,
        cpus: int=None, 
        worker_id: int=None,
        core_config: Dict[str, Any]={}
    ) -> None:
        self.transition_types: Dict[Tuple[StageTypes, StageTypes], Coroutine] = transition_types
        self.core_config = core_config
        self.graph_name = graph_name
        self.graph_id = graph_id
        self.generated_stages = {}
        self.transitions = {}
        self.instances_by_type: Dict[str, List[Stage]] = {}
        self.cpus = cpus
        self.worker_id = worker_id
        self.loop = asyncio.get_event_loop()
        self.hooks_by_type: Dict[HookType, Dict[str, Hook]] = defaultdict(dict)

        self.logging = HedraLogger()
        self.logging.initialize()

        self._thread_id = threading.current_thread().ident
        self._process_id = os.getpid()

        self._graph_metadata_log_string = f'Graph - {self.graph_name}:{self.graph_id} - thread:{self._thread_id} - process:{self._process_id} - '

    def generate_stages(self, stages: Dict[str, Stage]):

        stages_count = len(stages)
        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Generating - {stages_count} - stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Generating - {stages_count} - stages')

        self.instances_by_type = {}

        for stage in stages.values():
            self.instances_by_type[stage.stage_type] = []

        stage_types_count = len(self.instances_by_type)
        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Found - {stage_types_count} - unique stage types')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Found - {stage_types_count} - unique stage types')

        self.generated_stages: Dict[str, Stage] = {
            stage_name: stage() for stage_name, stage in stages.items()
        }

        for stage in self.generated_stages.values():
            stage.core_config = self.core_config
            stage.graph_name = self.graph_name
            stage.graph_id = self.graph_id

            stage.workers = self.cpus
            stage.worker_id = self.worker_id

            for hook_shortname, hook in registrar.reserved[stage.name].items():
                    hook._call = hook._call.__get__(stage, stage.__class__)
                    setattr(stage, hook_shortname, hook._call)

            methods = inspect.getmembers(stage, predicate=inspect.ismethod) 

            for _, method in methods:
                method_name = method.__qualname__
                hook: Hook = registrar.all.get(method_name)
                
                if hook:
                    hook._call = hook._call.__get__(stage, stage.__class__)
                    setattr(stage, hook.shortname, hook._call)

                    if inspect.ismethod(hook.call) is False:
                        hook.call = hook.call.__get__(stage, stage.__class__)
                        setattr(stage, hook.shortname, hook.call)

                    hook.stage = stage.name
                    hook.stage_instance: Stage = stage
                    
                    self.hooks_by_type[hook.hook_type][hook.name] = hook
                    stage.hooks[hook.hook_type].append(hook)
            
            self.instances_by_type[stage.stage_type].append(stage)

        event_hooks: List[EventHook] = list(self.hooks_by_type.get(HookType.EVENT, {}).values())
        for event_hook in event_hooks:
            for target_hook_name in event_hook.names:    
                target_hook = registrar.all.get(target_hook_name)
                
                if target_hook:
                    self.logging.filesystem.sync['hedra.core'].info(
                        f'{self._graph_metadata_log_string} - Appendng Event - {event_hook.name}:{event_hook.hook_id} - to target Stage - {target_hook.stage}:{target_hook.stage_instance.stage_id} Event Hooks'
                    )
                    
                    event = Event(target_hook, event_hook)
                    target_hook.stage_instance.hooks[HookType.EVENT].append(event)
                    registrar.all[event.name] = event
               
    
        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Successfully generated - {stages_count} - stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Successfully generated - {stages_count} - stages')

    def build_transitions_graph(self, topological_generations: List[List[str]], graph: networkx.Graph):

        self.logging.hedra.sync.debug(f'Buiding transitions matrix')
        self.logging.filesystem.sync['hedra.core'].debug(f'Buiding transitions matrix')

        transitions: List[List[Transition]] = []
        plugins: Dict[PluginType, Dict[str, Union[EnginePlugin, ReporterPlugin]]] = {
            PluginType.ENGINE: {},
            PluginType.OPTIMIZER: {},
            PluginType.PERSONA: {},
            PluginType.REPORTER: {}
        }

        for isolate_stage_name in networkx.isolates(graph):
            raise IsolatedStageError(
                self.generated_stages.get(isolate_stage_name)
            )
        
        for generation in topological_generations:

            generation_transitions = []

            stage_pool_size = self.cpus

            stages = {
                stage_name: self.generated_stages.get(stage_name) for stage_name in generation
            }
            parallel_stages = []

            no_workers_stages = [
                StageTypes.WAIT, 
                StageTypes.IDLE
            ]

            stages_count = len(stages)
            self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Provisioning workers - {stages_count} - stages')
            self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Provisioning workers- {stages_count} - stages')

            for stage in stages.values():

                for plugin_name, plugin in stage.plugins.items():
                    plugins[plugin.type][plugin_name] = plugin

                if stage.allow_parallel is False and stage.stage_type not in no_workers_stages:
                    stage.workers = 1
                    stage_pool_size -= 1

                    self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Stage - {stage.name} - provisioned - {stage.workers} - workers')
                    self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Stage - {stage.name} - provisioned - {stage.workers} - workers')

                    stage.executor = BatchExecutor(stage.workers)

                else:
                    parallel_stages.append((
                        stage.name,
                        stage
                    ))
            
            if len(parallel_stages) > 0:
                batch_executor = BatchExecutor(max_workers=stage_pool_size)

                batched_stages: List[Tuple[str, Stage, int]] = batch_executor.partion_stage_batches(parallel_stages)
                
                for _, stage, assigned_workers_count in batched_stages:

                    self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Stage - {stage.name} - provisioned - {assigned_workers_count} - workers')
                    self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Stage - {stage.name} - provisioned - {assigned_workers_count} - workers')

                    stage.workers = assigned_workers_count
                    stage.executor = BatchExecutor(max_workers=assigned_workers_count)

                    stages[stage.name] = stage

            for stage in stages.values():

                stage.plugins_by_type = plugins

                neighbors = list(graph.neighbors(stage.name))

                neighbors_count = len(neighbors)
                self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Discovered - {neighbors_count} - neighboring stages for stage - {stage.name}')
                self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Discovered - {neighbors_count} - neighboring stages for stage - {stage.name}')
                
                for neighbor in neighbors:
                    neighbor_stage = self.generated_stages.get(neighbor)

                    transition_action = self.transition_types.get((
                        stage.stage_type,
                        neighbor_stage.stage_type
                    ))

                    if transition_action == invalid_transition or transition_action == invalid_idle_transition:
                        invalid_transition_error, _ = self.loop.run_until_complete(
                            transition_action(stage, neighbor_stage)
                        )

                        raise invalid_transition_error

                    self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Created transition from - {stage.name} - to - {neighbor_stage.name}')
                    self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Created transition from - {stage.name} - to - {neighbor_stage.name}')

                    transition = Transition(
                        transition_action,
                        stage,
                        neighbor_stage
                    )

                    generation_transitions.append(transition)

            if len(generation_transitions) > 0:
                transitions.append(generation_transitions)

        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Transition matrix assemmbly complete')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Transition matrix assemmbly complete')

        return transitions

    def map_to_setup_stages(self, graph: networkx.DiGraph):

        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Mapping stages to requisite Setup stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Mapping stages to requisite Setup stages')
        
        idle_stages = self.instances_by_type.get(StageTypes.IDLE)
        for idle_stage in idle_stages:
            idle_stage.context.stages = {}
            idle_stage.context.visited = []
            idle_stage.context.results = {}
            idle_stage.context.results_stages = []
            idle_stage.context.summaries = {}
            idle_stage.context.paths = {}
            idle_stage.context.path_lengths = {}
            
        idle_stage_name = idle_stage.__class__.__name__

        complete_stage = self.instances_by_type.get(StageTypes.COMPLETE)[0]

        for stage_type in StageTypes:

            idle_stage.context.stages[stage_type] = {}

            for stage in self.instances_by_type.get(stage_type, []):

                stage_name = stage.__class__.__name__

                has_path = networkx.has_path(
                    graph, 
                    idle_stage_name,
                    stage_name
                )

                if has_path:
                    idle_stage.context.stages[stage_type][stage_name] = stage
                    paths = networkx.all_shortest_paths(graph, stage_name, complete_stage.name)
                
                    stage_paths = []
                    for path in paths:
                        stage_paths.extend(path)
                    
                    idle_stage.context.paths[stage_name] = stage_paths

                    path_lengths = networkx.all_pairs_shortest_path_length(graph)

                    stage_path_lengths = {}
                    for path_stage_name, path_lengths_set in path_lengths:

                        del path_lengths_set[path_stage_name]
                        stage_path_lengths[path_stage_name] = path_lengths_set

                    idle_stage.context.path_lengths[stage_name] = stage_path_lengths.get(stage_name)

        for stage in self.generated_stages.values():
            for idle_stage in idle_stages:
                stage.context = idle_stage.context

        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Mapped stages to requisite Setup stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Mapped stages to requisite Setup stages')
    
    def create_error_transition(self, error: Exception):

        from_stage = error.from_stage
            
        error_transition = self.transition_types.get((
            from_stage.stage_type,
            StageTypes.ERROR
        ))

        error_stage = Error()
        error_stage.graph_name = self.graph_name
        error_stage.graph_id = self.graph_id
        error_stage.error = error

        return Transition(
            error_transition,
            from_stage,
            error_stage
        )