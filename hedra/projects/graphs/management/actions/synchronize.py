import datetime
from typing import List
from .action import RepoAction
from .config import RepoConfig


class Syncrhonize(RepoAction):

    def __init__(self, config: RepoConfig, graph_files: List[str]) -> None:
        super(
            Syncrhonize,
            self
        ).__init__(config, graph_files)

    def execute(self):

        self._setup()
        self.remote.fetch()

        self._checkout()


        current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        pre_sync_message = f"Hedra graph update: {self.config.path} - {current_time}" 

        self.repo.git.add(A=True)
        self.repo.index.commit(pre_sync_message)
        self.remote.pull(self.branch.name, rebase=True)

        self.repo.index.add(self.discovered_files)

        sync_message = self.config.sync_message

        if sync_message is None:
            current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            sync_message = f"Hedra graph update: {self.config.path} - {current_time}" 

        self.repo.index.commit(sync_message)

        self.remote.push()


