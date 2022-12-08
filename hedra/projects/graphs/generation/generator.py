import inspect

from hedra.core.graphs.stages import (
    Analyze,
    Execute,
    Setup,
    Submit
)

from hedra.reporting.types import (
    JSONConfig
)

from .stages import (
    AnalyzeStage,
    ExecuteStage,
    SetupStage,
    SubmitResultsStage
)


class Generator:

    def __init__(self) -> None:

        self.hooks = [
            'action',
            'depends'
        ]

        self.stages = [
            SetupStage,
            ExecuteStage,
            AnalyzeStage,
            SubmitResultsStage
        ]

        self.stage_types = [
            Setup.__name__,
            Execute.__name__,
            Analyze.__name__,
            Submit.__name__
        ]

        self.reporter_types = [
            JSONConfig.__name__
        ]

    def generate_imports(self):

        generated_imports = []
        generated_imports.extend(self.stage_types)
        generated_imports.extend(self.reporter_types)
        generated_imports.extend(self.hooks)

        serialized_imports = '\n\t'.join([
            f'{import_name},' for import_name in generated_imports
        ])
        
        return f'from hedra import (\n\t{serialized_imports}\n)'

    def generate_stages(self):

        generated_stages = []
        for idx, stage_type in enumerate(self.stages):
            serialized_stage = inspect.getsource(stage_type)

            if idx > 0:
                generated_stages.append(f'@depends({self.stages[idx-1].__name__})\n{serialized_stage}')
            else:
                generated_stages.append(serialized_stage)

        serialized_stages = '\n\n'.join(generated_stages)

        return serialized_stages