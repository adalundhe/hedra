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
            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Creating repo directory at path - {self.config.path}')
            os.makedirs(self.config.path)

        try:
            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Re-initializing repo')
            self._setup()

            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Updating .gitignore')
            self._update_ignore()

            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Setting and fetching new branches from remote')
            self.remote = self.repo.remote(self.config.remote)
            self.remote.fetch()

            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Checking out branch - {self.config.branch} - and updating remote tracking')
            self._checkout()

        except Exception:

            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Initializing new repository at - {self.config.path}')
            self.repo = Repo.init(self.config.path)

            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Updating .gitignore')
            self._update_ignore()

            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Adding {len(self.discovered_files)} to new repo in commit')
            self.repo.index.add(self.discovered_files)
            self.repo.index.commit(f'Initialized new graph repo at - {self.config.path}')


            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Setting remote as - {self.config.remote} - with URL - {self.config.uri}')
            remote_names = [
                remote.name for remote in self.repo.remotes
            ]
            if self.config.remote not in remote_names:
                self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Creating new remote as - {self.config.remote}')
                self.repo.create_remote(self.config.remote, self.config.uri)

            self.remote = self.repo.remote(self.config.remote)
            self.remote.set_url(self.config.uri)
        
            self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Checking out branch - {self.config.branch} - and updating remote tracking')
            self._checkout()

        
