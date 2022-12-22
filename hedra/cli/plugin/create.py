import os
from hedra.projects.generation import PluginGenerator
from hedra.cli.exceptions.plugin.create import InvalidPluginType
from hedra.logging import HedraLogger
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


def create_plugin(plugin_type: str, path: str, log_level: str):

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

    logger['console'].sync.info(f'Creating new - {plugin_type} - plugin at - {path}.')

    generator = PluginGenerator()
    generated_plugin_data = None

    if plugin_type in generator.generator_types:
        generated_plugin_data = generator.generate_plugin(plugin_type)

    else:
        raise InvalidPluginType(
            plugin_type,
            list(generator.generator_types.keys())
        )

    logger['console'].sync.info('Saving template.')

    with open(path, 'w') as generated_plugin:
        generated_plugin.write(f'{generated_plugin_data}\n')

    logger['console'].sync.info('\nPlugin generated!\n')
    