import os
import glob
from typing import List
from pathlib import Path
from git.repo import Repo
from .action import RepoAction
from .config import RepoConfig


class Initialize(RepoAction):

    def __init__(self, config: RepoConfig) -> None:
        super(
            Initialize,
            self
        ).__init__(config)

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
            
            graph_files = self._discover_graph_files()

            self._update_ignore()

            self.repo.index.add('.gitignore')
            self.repo.index.add(graph_files)

            self.repo.index.commit(f'Initialized new graph repo at - {self.config.path}')

            self.repo.create_remote(self.config.remote, self.config.uri)
            self.remote = self.repo.remote(self.config.remote)
        
            self._checkout()

        self.remote.push(self.config.branch, set_upstream=True)