import os
import json
from urllib.parse import urlparse
from hedra.projects.management import GraphManager
from hedra.projects.management.graphs.actions import RepoConfig
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


def create_project(
    url: str, 
    project_name: str,
    path: str,
    username: str, 
    password: str,
    bypass_connection_validation: bool,
    connection_validation_retries: int,
    log_level: str,
):

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

    hedra_config_filepath = os.path.join(
        path,
        '.hedra.json'
    )

    logger['console'].sync.info(f'Checking if project exists at - {path}...')

    project_uninitialized = os.path.exists(hedra_config_filepath) is False
    if project_uninitialized:

        logger['console'].sync.info('No project found! Creating project directories and files.')

        project_directory = os.path.join(path, project_name)
        if os.path.exists(project_directory) is False:
            os.mkdir(project_directory)
        
        package_file = open(f'{project_directory}/__init__.py', 'w')
        package_file.close()

        tests_directory = os.path.join(project_directory, 'tests')
        os.mkdir(tests_directory)

        package_file = open(f'{tests_directory}/__init__.py', 'w')
        package_file.close()

        plugins_directory = os.path.join(project_directory, 'plugins')
        os.mkdir(plugins_directory)

        package_file = open(f'{plugins_directory}/__init__.py', 'w')
        package_file.close()

        log_directory = f'{os.getcwd()}/logs'

        if os.path.exists(log_directory) is False:
            os.mkdir(log_directory)

        elif os.path.isdir(log_directory) is False:
            os.remove(log_directory)
            os.mkdir(log_directory)

        parsed_url = urlparse(url)
        repo_url = f'{parsed_url.scheme}://{username}:{password}@{parsed_url.hostname}{parsed_url.path}'

        logger['console'].sync.info('Initializing project manager.')

        repo_config = RepoConfig(
            path,
            repo_url,
            branch='main',
            remote='origin',
            username=username,
            password=password
        )

        manager = GraphManager(repo_config, log_level=log_level)
        discovered = manager.discover_graph_files()

        new_graphs_count = len(discovered['graphs'])
        new_plugins_count = len(discovered['plugins'])

        logger['console'].sync.info(f'Found - {new_graphs_count} - new graphs and - {new_plugins_count} - plugins.')

        logger['console'].sync.info(f'Linking project to remote at - {url}.')

        workflow_actions = [
            'initialize',
            'create-gitignore'
        ]

        manager.execute_workflow(workflow_actions)

        hedra_config = {
            "name": project_name,
            "core": {
                "bypass_connection_validation": bypass_connection_validation,
                "connection_validation_retries": connection_validation_retries
            },
            'project': {
                'project_url': url,
                'project_username': username,
                'project_password': password
            },
            **discovered
        }

        logger['console'].sync.info('Saving project state to .hedra.json config.')

        with open(hedra_config_filepath, 'w') as hedra_config_file:
                json.dump(hedra_config, hedra_config_file, indent=4)

        logger['console'].sync.info('Project created!')

    else:
        logger['console'].sync.info(f'Found existing project at - {path}. Exiting...\n')
