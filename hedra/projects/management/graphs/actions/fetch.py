import datetime
from typing import List
from .action import RepoAction
from .config import RepoConfig


class Fetch(RepoAction):

    def __init__(self, config: RepoConfig, graph_files: List[str]) -> None:
        super(
            Fetch,
            self
        ).__init__(config, graph_files)

    def execute(self):
        self._pull_from_remote()
        self._checkout()
