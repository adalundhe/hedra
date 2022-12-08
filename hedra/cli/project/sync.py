import os
import json
import sys
import glob
import inspect
import importlib
import ntpath
import json
from pathlib import Path
from typing import List, Dict
from hedra.core.graphs.stages.stage import Stage
from urllib.parse import urlparse
from hedra.projects.graphs.management import GraphManager
from hedra.projects.graphs.management.actions import RepoConfig
from hedra.cli.exceptions.graph.sync import NotSetError


def sync_project(
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

    repo_config = RepoConfig(
        path,
        repo_url,
        branch=branch,
        remote=remote,
        sync_message=sync_message,
        username=username,
        password=password
    )

    manager = GraphManager(repo_config)
    discovered = manager.discover_graph_files()

    workflow_actions = [
        'initialize',
        'synchronize'
    ]

    
    manager.execute_workflow(workflow_actions)

    hedra_project_config.update({
        'project_url': url,
        'project_username': username,
        'project_password': password,
        'project_branch': branch,
        'project_remote': remote
    })

    hedra_config.update(discovered)

    hedra_config['project'] = hedra_project_config
    with open(hedra_config_filepath, 'w') as hedra_config_file:
            hedra_config = json.dump(hedra_config, hedra_config_file, indent=4)


