import datetime
from .action import RepoAction
from .config import RepoConfig


class Syncrhonize(RepoAction):

    def __init__(self, config: RepoConfig) -> None:
        super(
            Syncrhonize,
            self
        ).__init__(config)

    def execute(self):

        self._setup()
        self.remote.fetch()

        self._checkout()
        self.remote.pull(self.config.branch)

        graph_files = self._discover_graph_files()
        self.repo.index.add(graph_files)

        sync_message = self.config.sync_message

        if sync_message is None:
            current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            sync_message = f"Hedra graph update: {self.config.path} - {current_time}" 

        self.repo.index.commit(sync_message)

        self.remote.push()


