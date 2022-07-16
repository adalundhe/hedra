import asyncio
import inspect
import networkx
from typing import Dict, List
from hedra.test.stages.stage import Stage
from hedra.test.stages.types import StageTypes
from .validation import (
    validate_analyze_stage,
    validate_checkpoint_stage,
    validate_execute_stage,
    validate_optimize_stage,
    validate_setup_stage,
    validate_teardown_stage
)


class Pipeline:

    def __init__(self, stages: List[Stage]) -> None:
        
        self.stages = networkx.DiGraph()
        
        self.stage_types = {
            subclass.stage_type: subclass for subclass in Stage.__subclasses__()
        }

        self.stages_by_name = {stage.__name__: stage for stage in stages}
        
        self.instances: Dict[StageTypes, List[Stage]] = {}

        for stage in self.stage_types.values():
            stage_instances = stage.__subclasses__()
            self.instances[stage.stage_type] = stage_instances

        self.stages.add_nodes_from([
            (
                stage_name, 
                {"stage": stage}
            ) for stage_name, stage in self.stages_by_name.items()
        ])

        for stage in stages:
            for dependency in stage.dependencies:
                if self.stages.nodes.get(dependency.__name__):
                    self.stages.add_edge(dependency.__name__, stage.__name__)

  
        self.execution_order = [
            generation for generation in networkx.topological_generations(self.stages)
        ]

    def validate(self):
        if len(self.instances.get(StageTypes.SETUP)) < 1:
            raise Exception('Error: - Your graph must contain at least one Setup stage.')

        if len(self.instances.get(StageTypes.ANALYIZE)) < 1:
           self._append_stage(StageTypes.ANALYIZE)

        if len(self.instances.get(StageTypes.CHECKPOINT)) < 1:
            self._append_stage(StageTypes.CHECKPOINT)

        for instance in self.instances.get(StageTypes.SETUP):
            validate_setup_stage(instance)

        for instance in self.instances.get(StageTypes.OPTIMIZE):
            validate_optimize_stage(instance)
            
        for instance in self.instances.get(StageTypes.EXECUTE):
            validate_execute_stage(instance)

        for instance in self.instances.get(StageTypes.TEARDOWN):
            validate_teardown_stage(instance)

        for instance in self.instances.get(StageTypes.ANALYIZE):
            validate_analyze_stage(instance)

        for instance in self.instances.get(StageTypes.CHECKPOINT):
            validate_checkpoint_stage(instance)

    async def run(self):

        for stage_group in self.execution_order:
            stages = [
                self.stages_by_name.get(
                    stage_name
                )() for stage_name in stage_group 
            ]

            if len(stages) < 2:
                stage = stages.pop()
                
                await stage.configure()
                await stage.run()
                await stage.close()

            else:

                await asyncio.gather(*[
                    asyncio.create_task(
                        stage.configure()
                    ) for stage in stages
                ])

                await asyncio.gather(*[
                    asyncio.create_task(
                        stage.run()
                    ) for stage in stages
                ])

                await asyncio.gather(*[
                    asyncio.create_task(
                        stage.close()
                    ) for stage in stages
                ])


    def _append_stage(self, stage_type: StageTypes):

        appended_stage = self.stage_types.get(stage_type)
        last_cut = self.execution_order[-1]

        for stage_name in last_cut:
            stage = self.stages_by_name.get(stage_name)

            appended_stage.dependencies.append(stage)
            appended_stage.all_dependencies.append(stage)

            appended_stage.all_dependencies.extend(
                self.stages_by_name.get(stage_name).all_dependencies
            )

        appended_stage.all_dependencies = list(set(appended_stage.all_dependencies))
        self.stages.add_node(appended_stage.__name__, stage=appended_stage)

        for stage in last_cut:
            self.stages.add_edge(stage, appended_stage.__name__)

        self.execution_order = [
            generation for generation in networkx.topological_generations(self.stages)
        ]

        self.stages_by_name[appended_stage.__name__] = appended_stage
        self.instances[appended_stage.stage_type].append(appended_stage)
