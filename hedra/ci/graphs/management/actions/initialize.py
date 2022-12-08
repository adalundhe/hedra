import os
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
            self.remote.fetch()

        except Exception:

            self.repo = Repo()

            self.repo.create_remote(self.config.remote, self.config.uri)
            self.remote = self.repo.remote(self.config.remote)

        
        self._checkout()