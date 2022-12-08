import click
import psutil
import os
from hedra.cli.graph import (
    check_graph,
    run_graph,
    sync_graphs,
    discover_graphs
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
    help="Synchronize changes to graphs at the specified path with those in the specified git repository.\nIf the repository doesn't exist locally it is cloned."
)
@click.argument(
    'url'
)
@click.option(
    '--path',
    default=os.getcwd(),
    help='Path to graph repository.'
)
@click.option(
    '--branch',
    default='main',
    help='Git repository branch.'
)
@click.option(
    '--remote',
    default='origin',
    help='Git repository remote.'
)
@click.option(
    '--sync-message',
    help='Message for git commit.'
)
@click.option(
    '--username',
    help='Git repository username.'
)
@click.option(
    '--password',
    help='Git repository password'
)
def sync(
    url: str, 
    path: str,
    branch: str, 
    remote: str, 
    sync_message: str, 
    username: str, 
    password: str
):
    sync_graphs(
        url,
        path,
        branch,
        remote,
        sync_message,
        username,
        password
    )


@graph.command(
    help='Creates basic scaffolding for a test graph at the specified path.'
)
@click.argument('path')
def create(path: str):
    pass


@graph.command(
    help='Recursively searches the current directory or specified path for graph files and stores them in a .graphs.json file.'
)
@click.option(
    '--path',
    default=os.getcwd(),
    help='Path to search for graph files.'
)
def discover(path: str):
    discover_graphs(path)