import click
from hedra.cli.plugin import create_plugin


@click.group(help='Commands for creating and managing Hedra plugins.')
def plugin():
    pass



@plugin.command()
@click.argument('plugin_type')
@click.argument('path')
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
def create(plugin_type: str, path: str, log_level: str):
    create_plugin(plugin_type, path, log_level)