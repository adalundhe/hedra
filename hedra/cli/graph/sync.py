from hedra.ci.graphs.management import GraphManager
from hedra.ci.graphs.management.actions import RepoConfig


def sync_graphs(
    url: str, 
    path: str,
    branch: str, 
    remote: str, 
    sync_message: str, 
    username: str, 
    password: str
):

    repo_config = RepoConfig(
        path,
        url,
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
