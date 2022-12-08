import os
from pathlib import Path
from typing import List
from git import RemoteReference
from git.repo import Repo
from git.remote import Remote
from .config import RepoConfig


class RepoAction:

    def __init__(self, config: RepoConfig, graph_files: List[str]) -> None:
        self.config = config
        self.repo = None
        self.remote = None
        self.branch = None
        self.git = None
        self.graph_files = graph_files

    def _setup(self):
        self.repo = Repo(self.config.path)
        self.remote = Remote(self.repo, self.config.remote)
        self.branch = self.repo.create_head(self.config.branch)
        self.git = self.repo.git

    def _checkout(self):

        if self.branch in self.repo.branches and self.branch.name != self.repo.head.name:
            self.branch.checkout()

        else:
            self.branch = self.repo.create_head(self.config.branch)

        self.repo.head.reference = self.branch

        remote_reference = RemoteReference(
            self.repo, 
            f"refs/remotes/{self.config.remote}/{self.branch.name}"
        )

        self.repo.head.reference.set_tracking_branch(remote_reference).checkout()

    def _update_ignore(self):

        license_file = os.path.join(self.config.path, 'LICENSE')
        readme_path = os.path.join(self.config.path, 'README.md')

        valid_files = [
            license_file,
            readme_path,
            *self.graph_files
        ]

        gitignore_path = f'{self.config.path}/.gitignore'

        existing_ignore_files = []
        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r') as hedra_gitignore:
                existing_ignore_files = hedra_gitignore.readlines()

        with open(gitignore_path, 'a+') as hedra_gitignore:

            candidate_filter_files = [
                str(path.resolve()) for path in Path(self.config.path).rglob('*') if '.git' not in str(path.resolve())
            ]

            filter_files: List[str] = []
            for candidate_filter_file in candidate_filter_files:

                candidate_filter_filepath = str(Path(candidate_filter_file).resolve())

                if candidate_filter_filepath not in valid_files and candidate_filter_filepath not in existing_ignore_files:
                    filter_files.append(
                        os.path.relpath(
                            candidate_filter_filepath,
                            self.config.path
                        )
                    )

            filter_files_data = '\n'.join(filter_files)

            if len(filter_files) > 0:
                hedra_gitignore.writelines(f'{filter_files_data}\n')
        
        self.repo.index.add('.gitignore')

