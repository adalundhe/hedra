import sys
import glob
import importlib
import ntpath
import os
import inspect
from pathlib import Path
from typing import List
from git import RemoteReference
from git.repo import Repo
from git.remote import Remote
from hedra.core.graphs.stages.stage import Stage
from hedra.plugins.types.common.plugin import Plugin
from .config import RepoConfig


class RepoAction:

    def __init__(self, config: RepoConfig) -> None:
        self.config = config
        self.repo = None
        self.remote = None
        self.branch = None
        self.git = None

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

        
        graph_files = self._discover_graph_files()

        gitignore_path = f'{self.config.path}/.gitignore'

        existing_ignore_files = []
        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r') as hedra_gitignore:
                existing_ignore_files = hedra_gitignore.readlines()

        with open(gitignore_path, 'a+') as hedra_gitignore:

            candidate_filter_files = glob.glob(self.config.path + '/**/*', recursive=True)

            filter_files: List[str] = []
            for candidate_filter_file in candidate_filter_files:

                candidate_filter_filepath = str(Path(candidate_filter_file).resolve())

                if candidate_filter_filepath not in graph_files and candidate_filter_filepath not in existing_ignore_files:
                    filter_files.append(candidate_filter_filepath)

            filter_files_data = '\n'.join(filter_files)

            if len(filter_files) > 0:
                hedra_gitignore.writelines(f'\n{filter_files_data}')
        
        self.repo.index.add('.gitignore')

    def _discover_graph_files(self) -> List[str]:

        candidate_graph_files = glob.glob(self.config.path + '/**/*.py', recursive=True)

        graph_files = []

        for candidate_graph_file_path in candidate_graph_files:

            package_dir = Path(candidate_graph_file_path).resolve().parent
            package_dir_path = str(package_dir)
            package_dir_module = package_dir_path.split('/')[-1]
            
            package = ntpath.basename(candidate_graph_file_path)
            package_slug = package.split('.')[0]
            spec = importlib.util.spec_from_file_location(f'{package_dir_module}.{package_slug}', candidate_graph_file_path)

            if candidate_graph_file_path not in sys.path:
                sys.path.append(str(package_dir.parent))

            module = importlib.util.module_from_spec(spec)
            sys.modules[module.__name__] = module

            try:
                spec.loader.exec_module(module)
            
                stage_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())
                plugin_decendants = list({cls.__name__: cls for cls in Plugin.__subclasses__()}.values())

                discovered = {}
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, Stage) and obj not in stage_decendants:
                        discovered[name] = obj

                if len(discovered) > 0:                
                    graph_files.append(candidate_graph_file_path)

            except Exception:
                pass

        
        return graph_files

