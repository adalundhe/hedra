import os
import json
import json
from urllib.parse import urlparse
from hedra.projects.management import GraphManager
from hedra.projects.management.graphs.actions import RepoConfig
from hedra.cli.exceptions.graph.sync import NotSetError
from hedra.logging import HedraLogger
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)



def sync_project(
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

    logging_manager.disable(
        LoggerTypes.HEDRA, 
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.FILESYSTEM,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logging_manager.update_log_level(log_level)

    logger = HedraLogger()
    logger.initialize()
    logger['console'].sync.info(f'Running project sync at - {path}...')

    hedra_config_filepath = os.path.join(
        path,
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_project_config = hedra_config.get('project', {})
    

    if url is None:
        url = hedra_project_config.get('project_url')
    
    if username is None:
        username = hedra_project_config.get('project_username')

    if password is None:
        password = hedra_project_config.get('project_password')

    if branch is None:
        branch = hedra_project_config.get('project_branch', 'main')

    if remote is None:
        remote = hedra_project_config.get('project_remote', 'origin')

    if url is None:
        raise NotSetError(
            path,
            'url',
            '--url'
        )

    if username is None:
        raise NotSetError(
            path,
            'username',
            '--username'
        )

    if password is None:
        raise NotSetError(
            path,
            'password',
            '--password'
        )

    parsed_url = urlparse(url)
    repo_url = f'{parsed_url.scheme}://{username}:{password}@{parsed_url.hostname}{parsed_url.path}'

    logger['console'].sync.info('Initializing project manager.')

    repo_config = RepoConfig(
        path,
        repo_url,
        branch=branch,
        remote=remote,
        sync_message=sync_message,
        username=username,
        password=password,
        ignore_options=ignore
    )
    
    manager = GraphManager(repo_config, log_level=log_level)
    discovered = manager.discover_graph_files()

    new_graphs_count = len({
        graph_name for graph_name in discovered['graphs'] if graph_name not in hedra_config['graphs']
    })

    new_plugins_count = len(({
        plugin_name for plugin_name in discovered['plugins'] if plugin_name not in hedra_config['plugins']
    }))

    logger['console'].sync.info(f'Found - {new_graphs_count} - new graphs and - {new_plugins_count} - plugins.')

    if local is False:

        logger['console'].sync.info(f'Synchronizing project state with remote at - {url}...')

        workflow_actions = [
            'initialize',
            'synchronize'
        ]

        
        manager.execute_workflow(workflow_actions)

    else:
        logger['console'].sync.info('Skipping remote sync.')

    hedra_project_config.update({
        'project_url': url,
        'project_username': username,
        'project_password': password,
        'project_branch': branch,
        'project_remote': remote
    })

    hedra_config.update(discovered)

    logger['console'].sync.info('Saving project state to .hedra.json config.')

    hedra_config['project'] = hedra_project_config
    with open(hedra_config_filepath, 'w') as hedra_config_file:
            hedra_config = json.dump(hedra_config, hedra_config_file, indent=4)
            
    logger['console'].sync.info('Sync complete!\n')

