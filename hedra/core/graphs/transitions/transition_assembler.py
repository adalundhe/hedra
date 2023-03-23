import asyncio
import networkx
import threading
import os
from typing import List, Dict, Union, Any, Tuple, Coroutine
from collections import defaultdict
from hedra.core.hooks.types.base.event_graph import EventGraph
from hedra.core.graphs.stages.base.import_tools import set_stage_hooks
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.error import Error
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.hooks.types.base.hook import Hook, HookType
from hedra.core.graphs.stages.base.parallel.batch_executor import BatchExecutor
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.graphs.transitions.exceptions.exceptions import (
    IsolatedStageError, 
    InvalidTransitionError
)
from hedra.logging import HedraLogger
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.reporter.reporter_plugin import ReporterPlugin
from hedra.plugins.types.plugin_types import PluginType
from .transition_group import TransitionGroup
from .transition import Transition
from .common.transtition_metadata import TransitionMetadata
from .common.base_edge import BaseEdge


class TransitionAssembler:

    def __init__(
        self, 
        transition_types, 
        graph_name: str=None,
        graph_path: str=None,
        graph_id: str=None,
        graph_skipped_stages: List[str]=[],
        cpus: int=None, 
        worker_id: int=None,
        core_config: Dict[str, Any]={}
    ) -> None:
        self.transition_types: Dict[Tuple[StageTypes, StageTypes], Coroutine] = transition_types
        self.core_config = core_config
        self.graph_name = graph_name
        self.graph_path = graph_path
        self.graph_id = graph_id
        self.graph_skipped_stages: List[str] = graph_skipped_stages
        self.generated_stages = {}
        self.transitions = {}
        self.instances_by_type: Dict[str, List[Stage]] = {}
        self.cpus = cpus
        self.worker_id = worker_id
        self.loop = asyncio.get_event_loop()
        self.hooks_by_type: Dict[HookType, Dict[str,Hook]] = defaultdict(dict)

        self.logging = HedraLogger()
        self.logging.initialize()

        self._thread_id = threading.current_thread().ident
        self._process_id = os.getpid()
        self.all_hooks = []
        self.edges_by_name: Dict[Tuple[str, str], BaseEdge] = {}
        self.adjacency_list: Dict[str, List[Transition]] = defaultdict(list)

        self._graph_metadata_log_string = f'Graph - {self.graph_name}:{self.graph_id} - thread:{self._thread_id} - process:{self._process_id} - '

    def generate_stages(self, stages: Dict[str, Stage]) -> None:

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

        generated_hooks = {}
        for stage in self.generated_stages.values():

            if stage.name in self.graph_skipped_stages:
                stage.skip = True

            stage.core_config = self.core_config
            stage.graph_name = self.graph_name
            stage.graph_path = self.graph_path
            stage.graph_id = self.graph_id

            stage.workers = self.cpus
            stage.worker_id = self.worker_id

            for hook_shortname, hook in registrar.reserved[stage.name].items():
                hook._call = hook._call.__get__(stage, stage.__class__)
                setattr(stage, hook_shortname, hook._call)

            stage = set_stage_hooks(stage, generated_hooks)

            for hook_type in stage.hooks:
                for hook in stage.hooks[hook_type]: 
                    self.hooks_by_type[hook.hook_type][hook.name] = hook
                
            self.instances_by_type[stage.stage_type].append(stage)
    
        events_graph = EventGraph(self.hooks_by_type)
        events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()

        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Successfully generated - {stages_count} - stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Successfully generated - {stages_count} - stages')

    def build_transitions_graph(self, topological_generations: List[List[str]], graph: networkx.DiGraph) -> List[TransitionGroup]:

        self.logging.hedra.sync.debug(f'Buiding transitions matrix')
        self.logging.filesystem.sync['hedra.core'].debug(f'Buiding transitions matrix')

        transitions: List[TransitionGroup] = []
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

            generation_transitions = TransitionGroup()

            stage_pool_size = self.cpus

            stages = {
                stage_name: self.generated_stages.get(stage_name) for stage_name in generation
            }
            parallel_stages = []

            no_workers_stages = [
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
                stage.dispatcher.assemble_action_and_task_subgraphs()

                neighbors = list(graph.neighbors(stage.name))

                neighbors_count = len(neighbors)
                self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Discovered - {neighbors_count} - neighboring stages for stage - {stage.name}')
                self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Discovered - {neighbors_count} - neighboring stages for stage - {stage.name}')
                
                for neighbor in neighbors:
                    neighbor_stage = self.generated_stages.get(neighbor)

                    transition_action: TransitionMetadata = self.transition_types.get((
                        stage.stage_type,
                        neighbor_stage.stage_type
                    ))

                    self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Created transition from - {stage.name} - to - {neighbor_stage.name}')
                    self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Created transition from - {stage.name} - to - {neighbor_stage.name}')

                    transition = Transition(
                        transition_action,
                        stage,
                        neighbor_stage
                    )


                    if transition_action.is_valid is False:
                        raise InvalidTransitionError(
                            transition.from_stage,
                            transition.to_stage
                        )

                   
                    transition.predecessors = list(graph.predecessors(stage.name))
                    transition.descendants = list(graph.successors(stage.name))
                    
                    self.adjacency_list[stage.name].append(transition)

                    self.edges_by_name[(transition.from_stage.name, transition.to_stage.name)] = transition.edge

                    generation_transitions.add_transition(transition)

            if generation_transitions.count > 0:
                transitions.append(generation_transitions)

        for transition_group in transitions:
            transition_group.adjacency_list = self.adjacency_list
            transition_group.edges_by_name = self.edges_by_name

            for transition in transition_group:
                transition.adjacency_list = self.adjacency_list
                transition.edges_by_name = self.edges_by_name

        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Transition matrix assemmbly complete')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Transition matrix assemmbly complete')

        return transitions

    def map_to_setup_stages(self, graph: networkx.DiGraph) -> None:

        
        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Mapping stages to requisite Setup stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Mapping stages to requisite Setup stages')

        idle_stages = self.instances_by_type.get(StageTypes.IDLE)
        for idle_stage in idle_stages:
            idle_stage.context = SimpleContext()
            idle_stage.context.stages = {}
            idle_stage.context.visited = []
            idle_stage.context.results = {}
            idle_stage.context.results_stages = []
            idle_stage.context.summaries = {}
            idle_stage.context.paths = {}
            idle_stage.context.path_lengths = {}
            
            idle_stage.name = idle_stage.__class__.__name__


        complete_stage = self.instances_by_type.get(StageTypes.COMPLETE)[0]

        stages_by_type = defaultdict(dict)
        for stage_type in self.instances_by_type:
            for stage in self.instances_by_type[stage_type]:
                stages_by_type[stage_type][stage.name] = stage

        all_paths = {}

        for stage_type in StageTypes:

            idle_stage.context.stages[stage_type] = {}

            for stage in self.instances_by_type.get(stage_type, []):

                stage_name = stage.__class__.__name__

                for neighbor in self.adjacency_list[stage.name]:
                    self.edges_by_name[(stage.name, neighbor.edge.destination.name)].stages_by_type = stages_by_type
                    paths = networkx.all_simple_paths(graph, stage_name, complete_stage.name)
                
                    stage_paths = []
                    for path in paths:
                        stage_paths.extend(path)
                    
                    all_paths[stage_name] = stage_paths
                    
                    path_lengths = networkx.all_pairs_shortest_path_length(graph)

                    stage_path_lengths = {}
                    for path_stage_name, path_lengths_set in path_lengths:

                        del path_lengths_set[path_stage_name]
                        stage_path_lengths[path_stage_name] = path_lengths_set

                    self.edges_by_name[(stage.name, neighbor.edge.destination.name)].path_lengths[stage_name] = stage_path_lengths.get(stage_name)

        for stage in self.generated_stages.values():
            for neighbor in self.adjacency_list[stage.name]:
                self.edges_by_name[(stage.name, neighbor.edge.destination.name)].all_paths = all_paths


        self.logging.hedra.sync.debug(f'{self._graph_metadata_log_string} - Mapped stages to requisite Setup stages')
        self.logging.filesystem.sync['hedra.core'].debug(f'{self._graph_metadata_log_string} - Mapped stages to requisite Setup stages')
    
    def create_error_transition(self, error: Exception) -> Transition:

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