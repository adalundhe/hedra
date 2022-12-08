import os
import click
from hedra.cli.project import (
    sync_project
)

@click.group(help='Commands for managing collections of Hedra graphs.')
def project():
    pass


@project.command(
    help="Synchronize changes to the project at the specified path."
)
@click.option(
    '--url',
    help='Git repository url.'
)
@click.option(
    '--path',
    default=os.getcwd(),
    help='Path to graph repository.'
)
@click.option(
    '--branch',
    help='Git repository branch.'
)
@click.option(
    '--remote',
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
    sync_project(
        url,
        path,
        branch,
        remote,
        sync_message,
        username,
        password
    )


@project.command(
    help='Creates basic scaffolding for a project at the specified path.'
)
@click.argument('path')
def create(path: str):
    pass

