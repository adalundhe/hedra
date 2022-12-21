import os
import click
from hedra.cli.project import (
    create_project,
    sync_project,
    get_project,
    about_project
)


@click.group(help='Commands for managing collections of Hedra graphs.')
def project():
    pass


@project.command(
    help='Creates a project at the specified path.'
)
@click.argument('url')
@click.option(
    '--project-name',
    default='hedra_tests',
    help='Name of project to create.'
)
@click.option(
    '--path',
    default=os.getcwd(),
    help='Path to graph repository.'
)
@click.option(
    '--username',
    help='Git repository username.'
)
@click.option(
    '--password',
    help='Git repository password'
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
    '--log-level',
    default='info',
    help='Set log level.'
)
def create(
    url: str, 
    project_name: str,
    path: str,
    username: str, 
    password: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
    log_level: str
):
    create_project(
        url,
        project_name,
        path,
        username,
        password,
        bypass_connection_validation,
        connection_validation_retries,
        log_level
    )



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
@click.option(
    '--ignore',
    help='Comma delimited list of files to add to the project .gitignore.'
)
@click.option(
    '--log-level',
    default='info',
    help='Set log level.'
)
@click.option(
    '--local',
    is_flag=True,
    show_default=True,
    default=False,
    help='Synchronize only local project state.'
)
def sync(
    url: str, 
    path: str,
    branch: str, 
    remote: str, 
    sync_message: str, 
    username: str, 
    password: str,
    ignore: str,
    log_level: str,
    local: bool
):
    sync_project(
        url,
        path,
        branch,
        remote,
        sync_message,
        username,
        password,
        ignore,
        log_level,
        local
    )


@project.command(
    help='Clone down remote project to the specified path'
)
@click.argument('url')
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
    '--username',
    help='Git repository username.'
)
@click.option(
    '--password',
    help='Git repository password'
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
    '--log-level',
    default='info',
    help='Set log level.'
)
def get(
    url: str, 
    path: str,
    branch: str, 
    remote: str, 
    username: str, 
    password: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
    log_level: str
):
    get_project(
        url,
        path,
        branch,
        remote,
        username,
        password,
        bypass_connection_validation,
        connection_validation_retries,
        log_level
    )
 

@project.command(
    help="Describe the project at the specified path"
)
@click.option(
    '--path',
    default=os.getcwd(),
    help='Path to graph repository.'
)
def about(
    path: str
):
    about_project(path)