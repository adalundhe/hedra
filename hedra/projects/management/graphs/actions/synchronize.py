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

        self.logger.hedra.sync.debug(f'SyncrhonizeAction: {self.action_id} - Re-initializing repo')
        self._setup()

        self.logger.hedra.sync.debug(f'SyncrhonizeAction: {self.action_id} - Fetchin latest branches from remote')
        self.remote.fetch()

        self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Checking out branch - {self.config.branch} - and updating remote tracking')
        self._checkout()

        current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        pre_sync_message = f"Hedra graph update: {self.config.path} - {current_time}" 

        self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Running pre-pull commit.')
        self.repo.git.add(A=True)
        self.repo.index.commit(pre_sync_message)

        self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Pulling latest from remote repo at - URI: {self.config.uri} - Branch: {self.config.branch}')
        self.remote.pull(self.branch.name, rebase=True)

        self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Updating .gitignore')
        self._update_ignore(force_create=True)

        self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Adding {len(self.discovered_files)} to repo in commit')
        self.repo.index.add(self.discovered_files)

        sync_message = self.config.sync_message

        if sync_message is None:
            current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            sync_message = f"Hedra graph update: {self.config.path} - {current_time}" 

        self.repo.index.commit(sync_message)

        self.logger.hedra.sync.debug(f'InitializeAction: {self.action_id} - Pushing project changes and updates to remote repo at - URI: {self.config.uri} - Branch: {self.config.branch}')
        self.remote.push(self.config.branch, set_upstream=True)


