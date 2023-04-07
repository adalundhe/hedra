
import asyncio
import uuid
import networkx
import threading
import os
import time
from typing import Dict, List, Any
from hedra.core.graphs.stages.base.exceptions.process_killed_error import ProcessKilledError
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.graphs.transitions.transition_group import TransitionGroup
from hedra.logging import HedraLogger
from .transitions import TransitionAssembler, local_transitions
from .status import GraphStatus



class Graph:
    status = GraphStatus.IDLE

    def __init__(
        self, 
        graph_name: str,
        stages: List[Stage], 
        config: Dict[str, Any]={},
        cpus: int=None, 
        worker_id: int=None
    ) -> None:
        self.execution_time = 0
        self.core_config = config

        self.graph_name = graph_name
        self.graph_path = config.get('graph_path')
        self.graph_id = str(uuid.uuid4())
        self.graph_skipped_stages = config.get('graph_skipped_stages', [])
        self.status = GraphStatus.INITIALIZING
        self.graph = networkx.DiGraph()
        self.logger = HedraLogger()
        self._thread_id = threading.current_thread().ident
        self._process_id = os.getpid()
        self.metadata_string = f'Graph - {self.graph_name}:{self.graph_id} - thread:{self._thread_id} - process:{self._process_id} - '

        self.logger.initialize()

        self.logger.hedra.sync.debug(f'{self.metadata_string} - Changed status to - {GraphStatus.INITIALIZING.name} - from - {GraphStatus.IDLE.name}')
        self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Changed status to - {GraphStatus.INITIALIZING.name} - {GraphStatus.IDLE.name}')

        self.transitions_graph = []
        self._transitions: List[TransitionGroup] = []
        self._results = None

        self.logger.hedra.sync.debug(f'{self.metadata_string} - Found - {len(stages)} - stages')
        self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Found - {len(stages)} - stages')

        self.stage_types = {
            subclass.stage_type: subclass for subclass in Stage.__subclasses__()
        }

        self.instances: Dict[StageTypes, List[Stage]] = {}
        for stage in self.stage_types.values():
            stage_instances = [stage for stage in stage.__subclasses__() if stage.__module__ == self.core_config.get('graph_module')]
            self.instances[stage.stage_type] = stage_instances

        self.stages: Dict[str, Stage] = {stage.__name__: stage for stage in stages}
        
        self.graph.add_nodes_from([
            (
                stage_name, 
                {"stage": stage}
            ) for stage_name, stage in self.stages.items()
        ])

        for stage in stages:
            
            self.logger.hedra.sync.debug(f'{self.metadata_string} - Adding dependencies for stage - {stage.__name__}')
            self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Adding dependencies for stage - {stage.__name__}')

            for dependency in stage.dependencies:
                if self.graph.nodes.get(dependency.__name__):
                    
                    self.logger.hedra.sync.debug(f'{self.metadata_string} - Adding edge from stage - {dependency.__name__} - to stage - {stage.__name__}')
                    self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Adding edge from stage - {dependency.__name__} - to stage - {stage.__name__}.')

                    self.graph.add_edge(dependency.__name__, stage.__name__)

        self.execution_order = [
            generation for generation in networkx.topological_generations(self.graph)
        ]

        self.runner = TransitionAssembler(
            local_transitions,
            graph_name=self.graph_name,
            graph_path=self.graph_path,
            graph_id=self.graph_id,
            graph_skipped_stages=self.graph_skipped_stages,
            cpus=cpus,
            worker_id=worker_id,
            core_config=self.core_config
        )

    def assemble(self):

        self.status = GraphStatus.ASSEMBLING

        self.logger.hedra.sync.debug(f'{self.metadata_string} - Changed status to - {GraphStatus.ASSEMBLING.name} - from - {GraphStatus.INITIALIZING.name}')
        self.logger.filesystem.sync['hedra.core'].info(f'{self.metadata_string} - Changed status to - {GraphStatus.ASSEMBLING.name} - from - {GraphStatus.INITIALIZING.name}')

        # A user will never specify an Idle stage. Instead, we prepend one to 
        # serve as the source node for a graphs, ensuring graphs have a 
        # valid starting point and preventing the user from forgetting things
        # like a Setup stage.

        self.logger.hedra.sync.debug(f'{self.metadata_string} - Prepending {StageTypes.IDLE.name} stage')
        self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Prepending {StageTypes.IDLE.name} stage')

        self._prepend_stage(StageTypes.IDLE)

        # If we haven't specified an Analyze stage for results aggregation,
        # append one.
        if len(self.instances.get(StageTypes.ANALYZE)) < 1:
            self.logger.hedra.sync.debug(f'{self.metadata_string} - Appending {StageTypes.ANALYZE.name} stage')
            self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Appending {StageTypes.ANALYZE.name} stage')

            self._append_stage(StageTypes.ANALYZE)

        # If we havent specified a Submit stage for save aggregated results,
        # append one.
        if len(self.instances.get(StageTypes.SUBMIT)) < 1:
            self.logger.hedra.sync.debug(f'{self.metadata_string} - Appending {StageTypes.SUBMIT.name} stage')
            self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Appending {StageTypes.SUBMIT.name} stage')

            self._append_stage(StageTypes.SUBMIT)

        # Like Idle, a user will never specify a Complete stage. We append
        # one to serve as the sink node, ensuring all Graphs executed can
        # reach a single exit point.
        self.logger.hedra.sync.debug(f'{self.metadata_string} - Appending {StageTypes.COMPLETE.name} stage')
        self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Appending {StageTypes.COMPLETE.name} stage')

        self._append_stage(StageTypes.COMPLETE)

        self.logger.hedra.sync.debug(f'{self.metadata_string} - Generating graph stages and transitions')
        self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Generating graph stages and transitions')

        self.runner.generate_stages(self.stages)
        self._transitions = self.runner.build_transitions_graph(self.execution_order, self.graph)
        self.runner.map_to_setup_stages(self.graph)

        self.logger.hedra.sync.debug(f'{self.metadata_string} - Assembly complete')
        self.logger.filesystem.sync['hedra.core'].debug(f'{self.metadata_string} - Assembly complete')

    async def run(self):

        execution_start = time.monotonic()

        await self.logger.hedra.aio.debug(f'{self.metadata_string} - Changed status to - {GraphStatus.RUNNING.name} - from - {GraphStatus.ASSEMBLING.name}')
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Changed status to - {GraphStatus.RUNNING.name} - from - {GraphStatus.ASSEMBLING.name}')
        
        self.status = GraphStatus.RUNNING

        for transition_group in self._transitions:

            is_idle_transition = False

            transition_stage_types = [transition.edge.source.stage_type for transition in transition_group]
            is_idle_transition = StageTypes.IDLE in transition_stage_types

            transition_group.sort_and_map_transitions()

            current_stages = ', '.join(
                list(set([transition.from_stage.name for transition in transition_group]))
            )

            self.logger.spinner.logger_enabled = True
            if is_idle_transition:
                self.logger.spinner.logger_enabled = False

            async with self.logger.spinner as status_spinner:
                
                if is_idle_transition is False:
                    await self.logger.spinner.append_message(f"Executing stages - {current_stages}")
                
                    for transition in transition_group:
                        await status_spinner.system.debug(f'{self.metadata_string} - Executing stage Transtition - {transition.transition_id} -  from stage - {transition.from_stage.name} - to stage - {transition.to_stage.name}')
                        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing stage Transition - {transition.transition_id} - from stage - {transition.from_stage.name} - to stage - {transition.to_stage.name}')
                        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing stage - {transition.from_stage.name}:{transition.from_stage.stage_id}')

                results = await asyncio.gather(*[
                    asyncio.create_task(transition.execute()) for transition in transition_group
                ])
                
                for transition in transition_group:
                    error = transition.edge.exception

                    if isinstance(error, ProcessKilledError):
                        self.status = GraphStatus.CANCELLED
                        return
                    
                    if error:

                        source_stage = transition.edge.source

                        self.status = GraphStatus.FAILED

                        await status_spinner.system.debug(f'{self.metadata_string} - Changed status to - {GraphStatus.FAILED.name} - from - {GraphStatus.RUNNING.name}')
                        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Changed status to - {GraphStatus.FAILED.name} - from - {GraphStatus.RUNNING.name}')

                        await status_spinner.system.error(f'{self.metadata_string} - Encountered error executing stage - {source_stage.name}:{source_stage.stage_id}')
                        await self.logger.filesystem.aio['hedra.core'].error(f'{self.metadata_string} - Encountered error executing stage - {source_stage.name}:{source_stage.stage_id}')

                        error_transtiton = self.runner.create_error_transition(
                            source_stage,
                            error
                        )

                        await error_transtiton.execute() 

                if self.status == GraphStatus.FAILED:
                    status_spinner.finalize()
                    await status_spinner.fail('Error')
                    break
                
                for transition in transition_group:
                    await status_spinner.system.debug(f'{self.metadata_string} - Completed stage Transtition - {transition.transition_id} -  from stage - {transition.from_stage.name} - to stage - {transition.to_stage.name}')


                if is_idle_transition is False:
                    completed_transitions_count = len(results)
                    await status_spinner.system.debug(f'{self.metadata_string} - Completed -  {completed_transitions_count} - transitions')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Completed -  {completed_transitions_count} - transitions')

                    status_spinner.group_finalize()

                    await status_spinner.ok('✔')

                results = None
            
        if self.status == GraphStatus.RUNNING:
            await self.logger.spinner.system.debug(f'{self.metadata_string} - Changed status to - {GraphStatus.COMPLETE.name} - from - {GraphStatus.RUNNING.name}')
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Changed status to - {GraphStatus.COMPLETE.name} - from - {GraphStatus.RUNNING.name}')

            self.status = GraphStatus.COMPLETE

        self.execution_time = execution_start - time.monotonic()

        for transition_group in self._transitions:
            for transition in transition_group:
                transition.edge.source.context = None
                transition.edge.destination.context = None
                transition.edge.history = None
            
            transition_group.destination_groups = None
            transition_group.transitions = None
            transition_group.transitions_by_type = None
            transition_group.edges_by_name = None
            transition_group.adjacency_list = None

    async def cleanup(self):
        pass

    def _append_stage(self, stage_type: StageTypes):

        appended_stage = self.stage_types.get(stage_type)
        last_cut = self.execution_order[-1]

        appended_stage.dependencies = list()

        for stage_name in last_cut:
            stage = self.stages.get(stage_name)

            appended_stage.dependencies.append(stage)

        self.graph.add_node(appended_stage.__name__, stage=appended_stage)

        for stage_name in last_cut:
            self.graph.add_edge(stage_name, appended_stage.__name__)

        self.execution_order = [
            generation for generation in networkx.topological_generations(self.graph)
        ]

        self.stages[appended_stage.__name__] = appended_stage
        self.instances[appended_stage.stage_type].append(appended_stage)

    def _prepend_stage(self, stage_type: StageTypes):
        prepended_stage = self.stage_types.get(stage_type)
        first_cut = self.execution_order[0]

        prepended_stage.dependencies = list()

        for stage_name in first_cut:
            stage = self.stages.get(stage_name)

            stage.dependencies.append(prepended_stage)

        self.graph.add_node(prepended_stage.__name__, stage=prepended_stage)

        for stage_name in first_cut:
            self.graph.add_edge(prepended_stage.__name__, stage_name)

        self.execution_order = [
            generation for generation in networkx.topological_generations(self.graph)
        ]

        self.stages[prepended_stage.__name__] = prepended_stage
        self.instances[prepended_stage.stage_type].append(stage_type)