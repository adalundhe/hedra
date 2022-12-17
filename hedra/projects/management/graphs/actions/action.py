import os
from pathlib import Path
from typing import List
from git import RemoteReference
from git.repo import Repo
from git.remote import Remote
from .config import RepoConfig


class RepoAction:

    def __init__(self, config: RepoConfig, discovered_files: List[str]) -> None:
        self.config = config
        self.repo = None
        self.remote = None
        self.branch = None
        self.git = None
        self.discovered_files = discovered_files

    def _pull_from_remote(self):
        self.repo = Repo.clone_from(self.config.uri, self.config.path)
        self.branch = self.repo.create_head(self.config.branch)
        self.remote = self.repo.remote(name=self.config.remote)
        self.git = self.repo.git

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

    def _update_ignore(self, force_create=False):

        license_file = os.path.join(self.config.path, 'LICENSE')
        readme_path = os.path.join(self.config.path, 'README.md')

        valid_files = [
            license_file,
            readme_path,
            *self.discovered_files
        ]

        gitignore_path = f'{self.config.path}/.gitignore'

        existing_ignore_files = []
        
        if os.path.exists(gitignore_path) or force_create:

            if os.path.exists(gitignore_path):
                with open(gitignore_path, 'r') as hedra_gitignore:
                    existing_ignore_files.extend([
                        existing_ignore_file.strip('\n') for existing_ignore_file in hedra_gitignore.readlines()
                    ])

            with open(gitignore_path, 'a+') as hedra_gitignore:
                
                filter_files: List[str] = []
                candidate_filter_files = [
                    str(path.resolve()) for path in Path(self.config.path).rglob('*') if '.git' not in str(path.resolve())
                ]

                existing_ignore_files.extend(
                    self.repo.ignored(candidate_filter_files)
                )
                
                for candidate_filter_file in candidate_filter_files:

                    candidate_filter_filepath = str(Path(candidate_filter_file).resolve())

                    candidate_relative_path = os.path.relpath(candidate_filter_filepath, self.config.path)
                    
                    valid_ignore_candidate = candidate_filter_filepath not in valid_files
                    not_already_ignored = candidate_filter_filepath not in existing_ignore_files
                    not_directory = os.path.isdir(candidate_filter_filepath) is False

                    if valid_ignore_candidate and not_already_ignored and not_directory:
                        filter_files.append(candidate_relative_path)

                for ignore_option in self.config.ignore_options:
                    if ignore_option not in existing_ignore_files:
                        filter_files.append(ignore_option)

                filter_files_data = '\n'.join([
                    filepath for filepath in filter_files
                ])

                if len(filter_files) > 0:
                    hedra_gitignore.writelines(f'\n{filter_files_data}\n')
                    
            self.repo.index.add('.gitignore')

