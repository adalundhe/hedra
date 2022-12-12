import click
import psutil
import os
from hedra.cli.graph import (
    check_graph,
    run_graph,
    create_graph
)

@click.group(help='Commands to run, lint, generate, and manage graphs.')
def graph():
    pass


@graph.command(help="Run a specified test file.")
@click.argument('path')
@click.option(
    '--cpus', 
    default=psutil.cpu_count(logical=False), 
    help='Number of CPUs to use. Default is the number of physical processesors available to the system.'
)
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
def run(path: str, cpus: int, log_level: str):
    run_graph(path, cpus, log_level)


@graph.command(help="Validate the specified test file.")
@click.argument('path')
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
def check(path: str, log_level: str):
    check_graph(path, log_level)


@graph.command(
    help='Creates basic scaffolding for a test graph at the specified path.'
)
@click.argument('path')
@click.option(
    '--stages',
    help='Optional comma delimited list of stages to generate for the graph.'
)
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
@click.option(
    '--log-directory',
    default=f'{os.getcwd()}/logs',
    help='Set log level.'
)
def create(path: str, stages: str, log_level: str, log_directory: str):
    create_graph(path, stages, log_level, log_directory)