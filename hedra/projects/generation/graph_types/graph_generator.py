from typing import List
from .stages import (
    AnalyzeStage,
    CheckpointStage,
    OptimizeStage,
    SetupStage,
    TeardownStage,
    ValidateStage
)
from .stages.execute import (
    ExecuteGraphQLStage,
    ExecuteGraphQLHttp2Stage,
    ExecuteHTTPStage,
    ExecuteHTTP2Stage,
    ExecutePlaywrightStage,
    ExecuteTaskStage,
    ExecuteUDPStage,
    ExecuteWebsocketStage
)

from .stages.submit import (
    SubmitJSONResultsStage,
    SubmitCSVResultsStage
)

from hedra.core.graphs.hooks import depends
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.projects.generation.generator import Generator


class GraphGenerator(Generator):

    def __init__(self) -> None:
        super().__init__({
            'analyze': AnalyzeStage,
            'checkpoint': CheckpointStage,
            'csv': SubmitCSVResultsStage,
            'graphql': ExecuteGraphQLStage,
            'graphql-http2': ExecuteGraphQLHttp2Stage,
            'http':ExecuteHTTPStage,
            'http2': ExecuteHTTP2Stage,
            'json': SubmitJSONResultsStage,
            'optimize': OptimizeStage,
            'playwright': ExecutePlaywrightStage,
            'setup': SetupStage,
            'task': ExecuteTaskStage,
            'teardown': TeardownStage,
            'udp': ExecuteUDPStage,
            'validate': ValidateStage,
            'websocket': ExecuteWebsocketStage,
            'depends': depends
        }, registrar.module_paths)

        self.valid_types = [
            'analyze',
            'checkpoint',
            'execute',
            'optimize',
            'setup',
            'submit',
            'teardown',
            'validate'
        ]

    def generate_graph(
        self, 
        stages: List[str],
        engine: str=None,
        persona: str=None,
        reporter: str=None
    ):

        if engine not in self.generator_types:
            engine = 'http'

        if reporter not in self.generator_types:
            reporter = 'json'

        for stage in stages:
            print(stage)

            generator_type = stage
            if stage == "execute":
                generator_type = engine

            elif stage == "submit":
                generator_type = reporter

            modules = self.gather_required_items(generator_type)

            self.collect_imports(generator_type, modules)

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
