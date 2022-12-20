import datetime
from typing import List
from .action import RepoAction
from .config import RepoConfig



class CreateGitignore(RepoAction):

    def __init__(self, config: RepoConfig, graph_files: List[str]) -> None:
        super(
            CreateGitignore,
            self
        ).__init__(config, graph_files)

    def execute(self):

        self.logger.hedra.sync.debug(f'CreateGitignore: {self.action_id} - creating .gitignore at path - {self.config.path}')
        self._setup()
        self._update_ignore(force_create=True)

        current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
        gitignore_mmessage = f"Hedra graph update: {self.config.path} - {current_time}" 


        self.logger.hedra.sync.debug(f'CreateGitignore: {self.action_id} - making local commit to add new .gitignore at path - {self.config.path}')
        self.repo.index.commit(gitignore_mmessage)