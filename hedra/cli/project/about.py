import os
import json
from typing import Dict, Any
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


def about_project(
    path: str
):
    logging_manager.disable(
        LoggerTypes.HEDRA, 
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.FILESYSTEM,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logger = HedraLogger()
    logger.initialize()
    logging_manager.logfiles_directory = os.getcwd()

    hedra_config_filepath = os.path.join(
        path,
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as config_file:
            hedra_config: Dict[str, Any] = json.load(config_file)

    else:
        logger.console.sync.error(f'No project found at path - {path}\n')
        exit(0)

    project_name = hedra_config.get('name')
    logger.console.sync.info(f'Project - {project_name}')

    logger.console.sync.info(f'\nGraphs:')
    hedra_graphs = hedra_config.get('graphs', {})

    if len(hedra_graphs) > 0:
        for graph_name, graph_path in hedra_graphs.items():
            logger.console.sync.info(f' - {graph_name} at {graph_path}')

    else:
        logger.console.sync.info(' - No graphs registered')

    logger.console.sync.info(f'\nPlugins:')
    hedra_plugins = hedra_config.get('plugins', {})

    if len(hedra_plugins) > 0:
        for plugin_name, plugin_path in hedra_plugins.items():
            logger.console.sync.info(f' - {plugin_name} at {plugin_path}')

    else:
        logger.console.sync.info(' - No plugins registered')


    logger.console.sync.info(f'\nCore Options:')
    hedra_options = hedra_config.get('core', {})
    
    if len(hedra_options) > 0:
        for opttion_name, option_value in hedra_options.items():
            logger.console.sync.info(f' - {opttion_name} set as {option_value}')

    else:
        logger.console.sync.info(' - No options set')


    logger.console.sync.info('')