import datetime
from typing import List
from hedra.logging import HedraLogger
from .action import RepoAction
from .config import RepoConfig


class Fetch(RepoAction):

    def __init__(self, config: RepoConfig, graph_files: List[str]) -> None:
        super(
            Fetch,
            self
        ).__init__(config, graph_files)

    def execute(self):
        self.logger.hedra.sync.debug(f'FetchAction: {self.action_id} - Cloning from remote - {self.config.uri} - to path - {self.config.path}')
        self._pull_from_remote()

        self.logger.hedra.sync.debug(f'FetchAction: {self.action_id} - Checking out branch - {self.config.branch} - and updating remote')
        self._checkout()

        self.logger.hedra.sync.debug(f'FetchAction: {self.action_id} - Updating .gitignore')
        self._update_ignore()
