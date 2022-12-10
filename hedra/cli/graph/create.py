import inspect
from typing import Optional
from hedra.projects.generation import GraphGenerator
from hedra.core.graphs.stages.stage import Stage
from hedra.cli.exceptions.graph.create import InvalidStageType


def create_graph(path: str, stages: Optional[str]):

    if stages is None:
        stages_list = [
            'setup',
            'execute',
            'analyze',
            'submit'
        ]

    else:
        stages_list = stages.split(',')

    
    generator = GraphGenerator()

    for stage in stages_list:
        if stage not in generator.generator_types:
            raise InvalidStageType(stage, [
                generator_type_name for generator_type_name, generator_type in generator.generator_types.items() if inspect.isclass(
                    generator_type
                ) and issubclass(generator_type, Stage)
            ])


    with open(path, 'w') as generated_test:
        generated_test.write(
            generator.generate_graph(stages_list)
        )