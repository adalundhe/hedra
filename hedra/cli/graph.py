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
    default=psutil.cpu_count(), 
    help='Number of CPUs to use. Default is the number of physical processesors available to the system.'
)
@click.option(
    '--skip',
    default='',
    help='Comma-delimited list of Stage names to skip.'
)
@click.option(
    '--retries',
    default=0,
    help='Global retries for graph.'
)
@click.option(
    '--show-summaries',
    default='',
    help='Comma-delimited list of results tables to show upon completion.'
)
@click.option(
    '--hide-summaries',
    default='',
    help='Comma-delimited list of results tables to omit upon completion.'
)
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
@click.option(
    '--log-directory',
    default=f'{os.getcwd()}/logs',
    help='Output directory for logfiles. If the directory does not exist it will be created.'
)
@click.option(
    '--bypass-connection-validation',
    is_flag=True,
    show_default=True,
    default=False,
    help="Skip Hedra's action connection validation."
)
@click.option(
    '--connection-validation-retries',
    default=3,
    help="Set the number of retries for connection validation."
)
@click.option(
    '--enable-latest',
    is_flag=True,
    show_default=True,
    default=False,
    help='Enable features marked as unstable.'
)
def run(
    path: str, 
    cpus: int, 
    skip: str,
    retries: int,
    show_summaries: str,
    hide_summaries: str,
    log_level: str, 
    log_directory: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
    enable_latest: bool,
):
    run_graph(
        path, 
        cpus,
        skip, 
        retries,
        show_summaries,
        hide_summaries,
        log_level, 
        log_directory,
        bypass_connection_validation,
        connection_validation_retries,
        enable_latest
    )


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
    '--engine',
    default='http',
    help='Engine to use in generated graph.'
)
@click.option(
    '--reporter',
    default='json',
    help='Reporter to use in generated graph.'
)
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
def create(
    path: str, 
    stages: str, 
    engine: str,
    reporter: str,
    log_level: str,
):
    create_graph(
        path, 
        stages, 
        engine,
        reporter,
        log_level
    )