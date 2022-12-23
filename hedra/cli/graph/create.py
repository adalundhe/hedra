import inspect
import os
import json
from pathlib import Path
from typing import Optional
from hedra.projects.generation import GraphGenerator
from hedra.core.graphs.stages.base.stage import Stage
from hedra.cli.exceptions.graph.create import InvalidStageType
from hedra.logging import HedraLogger
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


def create_graph(
    path: str, 
    stages: Optional[str], 
    engine: str,
    reporter: str,
    log_level: str
):

    logging_manager.disable(
        LoggerTypes.HEDRA, 
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.FILESYSTEM,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )
    logging_manager.update_log_level(log_level)

    logger = HedraLogger()
    logger.initialize()
    logging_manager.logfiles_directory = os.getcwd()

    logger['console'].sync.info(f'Creating new graph at - {path}.')

    if stages is None:
        stages_list = [
            'setup',
            'execute',
            'analyze',
            'submit'
        ]

    else:
        stages_list = stages.split(',')
    
    generated_stages_count = len(stages_list)
    generated_stages = ''.join([
        f'\n-{stage}' for stage in stages_list
    ])   

    logger['console'].sync.info(f'Generating - {generated_stages_count} stages:{generated_stages}') 


    generator = GraphGenerator()

    for stage in stages_list:
        if stage not in generator.valid_types:
            raise InvalidStageType(stage, [
                generator_type_name for generator_type_name, generator_type in generator.generator_types.items() if inspect.isclass(
                    generator_type
                ) and issubclass(generator_type, Stage)
            ])


    with open(path, 'w') as generated_test:
        generated_test.write(
            generator.generate_graph(
                stages_list,
                engine=engine,
                reporter=reporter
            )
        )

    graph_name = path
    if os.path.isfile(graph_name):
        graph_name = Path(graph_name).stem

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_graphs = hedra_config.get('graphs', {})

    if hedra_graphs.get(graph_name) is None:
        hedra_graphs[graph_name] = str(Path(path).absolute().resolve())
        with open(hedra_config_filepath, 'w') as hedra_config_file:
            hedra_config['graphs'] = hedra_graphs
            json.dump(hedra_config, hedra_config_file, indent=4)   

    logger['console'].sync.info('\nGraph generated!\n')