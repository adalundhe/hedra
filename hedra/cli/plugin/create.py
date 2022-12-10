from hedra.projects.generation import PluginGenerator
from hedra.cli.exceptions.plugin.create import InvalidPluginType


def create_plugin(plugin_type: str, path: str):

    generator = PluginGenerator()

    generated_plugin_data = None

    if plugin_type in generator.generator_types:
        generated_plugin_data = generator.generate_plugin(plugin_type)

    else:
        raise InvalidPluginType(
            plugin_type,
            list(generator.generator_types.keys())
        )

    with open(path, 'w') as generated_plugin:
        generated_plugin.write(f'{generated_plugin_data}\n')
    