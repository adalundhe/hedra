import os
from typing import List
from git.repo import Repo
from .action import RepoAction
from .config import RepoConfig


class Initialize(RepoAction):

    def __init__(self, config: RepoConfig, graph_files: List[str]) -> None:
        super(
            Initialize,
            self
        ).__init__(config, graph_files)

    def execute(self):

        if os.path.exists(self.config.path) is False:
            os.makedirs(self.config.path)

        try:

            self._setup()

            self._update_ignore()
            self.remote = self.repo.remote(self.config.remote)
            self.remote.fetch()
            self._checkout()

        except Exception:

            self.repo = Repo.init(self.config.path)

            self._update_ignore()
            self.repo.index.add(self.discovered_files)

            self.repo.index.commit(f'Initialized new graph repo at - {self.config.path}')

            remote_names = [
                remote.name for remote in self.repo.remotes
            ]
            if self.config.remote not in remote_names:
                self.repo.create_remote(self.config.remote, self.config.uri)

            self.remote = self.repo.remote(self.config.remote)

            self.remote.set_url(self.config.uri)
        
            self._checkout()

        
