import os
import json
from urllib.parse import urlparse
from hedra.ci.graphs.management import GraphManager
from hedra.ci.graphs.management.actions import RepoConfig
from hedra.cli.exceptions.graph.sync import NotSetError


def sync_graphs(
    url: str, 
    path: str,
    branch: str, 
    remote: str, 
    sync_message: str, 
    username: str, 
    password: str
):

    hedra_config_filepath = os.path.join(
        path,
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    hedra_repo_config = hedra_config.get('repository', {})
    if url is None:
        url = hedra_repo_config.get('remote_url')
    
    if username is None:
        username = hedra_repo_config.get('remote_username')

    if password is None:
        password = hedra_repo_config.get('remote_password')

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

    repo_config = RepoConfig(
        path,
        repo_url,
        branch=branch,
        remote=remote,
        sync_message=sync_message,
        username=username,
        password=password
    )

    workflow_actions = [
        'initialize',
        'synchronize'
    ]

    manager = GraphManager()
    manager.execute_workflow(workflow_actions, repo_config)

    hedra_repo_config.update({
        'remote_url': url,
        'remote_username': username,
        'remote_password': password
    })

    hedra_config['repository'] = hedra_repo_config
    with open(hedra_config_filepath, 'w') as hedra_config_file:
            hedra_config = json.dump(hedra_config, hedra_config_file, indent=4)


