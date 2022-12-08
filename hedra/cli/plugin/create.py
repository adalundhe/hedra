from hedra.projects.plugins.generation import PluginGenerator
from hedra.cli.exceptions.plugin.create import InvalidPluginType


def create_plugin(plugin_type: str, path: str):
    print(plugin_type, path)

    generator = PluginGenerator()

    generated_plugin_data = None

    if plugin_type == 'persona':
        generated_plugin_data = generator.generate_persona()

    elif plugin_type == 'engine':
        generated_plugin_data = generator.generate_engine()

    elif plugin_type == 'optimizer':
        generated_plugin_data = generator.generate_optimizer()

    elif plugin_type == 'reporter':
        generated_plugin_data = generator.generate_reporter()

    else:
        raise InvalidPluginType(
            plugin_type,
            [
                'persona',
                'engine',
                'optimizer',
                'reporter'
            ]
        )

    with open(path, 'w') as generated_plugin:
        generated_plugin.write(f'{generated_plugin_data}\n')
    