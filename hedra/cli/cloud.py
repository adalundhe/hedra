import click


@click.group(help="Commands to run graphs on and manage distributed instances of Hedra.")
def cloud():
    pass


@cloud.group(help='Login to a Hedra Cloud cluster.')
@click.option(
    '--username',
    help='Hedra Cloud cluster username.'
)
@click.option(
    '--token',
    help='Hedra Cloud cluster auth token.'
)
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def login(username: str, auth_token: str, cluster: str):
    pass


@cloud.group(help='Run the specified graph on the Hedra Cloud cluster.')
@click.argument('graph_name')
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def run(graph_name: str, cluster: str):
    pass


@cloud.group(help='Trigger a project sync on the Hedra Cloud cluster.')
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def sync(cluster: str):
    pass


@cloud.group(help='Submit a test config to the Hedra Cloud cluster.')
@click.argument('config_path')
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def submit(config_path: str, cluster: str):
    pass


@cloud.group(help='Validate the specified graph on the Hedra Cloud cluster.')
@click.argument('graph_name')
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def check(graph_name: str, cluster: str):
    pass


@cloud.group(help='Follow test progress as it runs on the specified Hedra Cloud cluster.')
@click.argument('test_id')
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def watch(test_id: str, cluster: str):
    pass


@cloud.group(help='Update graph config for the specified graph on the specified Hedra Cloud cluster.')
@click.argument('graph_name')
@click.option(
    '--cluster',
    help='Hedra Cluster address.'
)
def update(graph_name: str, cluster: str):
    pass