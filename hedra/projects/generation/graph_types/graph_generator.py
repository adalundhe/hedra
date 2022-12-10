from typing import List
from .stages import (
    AnalyzeStage,
    ExecuteStage,
    SetupStage,
    SubmitResultsStage
)

from hedra.core.graphs.hooks import depends
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.projects.generation.generator import Generator


class GraphGenerator(Generator):

    def __init__(self) -> None:
        super().__init__({
            'analyze': AnalyzeStage,
            'execute':ExecuteStage,
            'setup': SetupStage,
            'submit': SubmitResultsStage,
            'depends': depends
        }, registrar.module_paths)

    def generate_graph(self, stages: List[str]):

        for stage in stages:
            modules = self.gather_required_items(stage)
            self.collect_imports(stage, modules)

        self.collect_imports(
            None,
            {
                'depends': depends
            }
        )
        
        self.serialize_items() 

        serialized_imports = '\n'.join([
            *self.serialized_global_imports,
            *self.serialized_local_imports
        ])

        for idx, serialized_stage in enumerate(self.serialized_locals):

            if idx > 0:
                previous_stage_name = self.locals[idx-1].__name__
                self.serialized_locals[idx] = f'@depends({previous_stage_name})\n{serialized_stage}'

        return '\n\n'.join([
            serialized_imports,
            *self.serialized_locals
        ])
