import click
import psutil
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
def run(path: str, cpus: int):
    run_graph(path, cpus)


@graph.command(help="Validate the specified test file.")
@click.argument('path')
def check(path: str):
    check_graph(path)


@graph.command(
    help='Creates basic scaffolding for a test graph at the specified path.'
)
@click.argument('path')
@click.option(
    '--stages',
    help='Optional comma delimited list of stages to generate for the graph.'
)
def create(path: str, stages: str):
    create_graph(path, stages)