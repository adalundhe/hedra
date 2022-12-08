import sys
import glob
import importlib
import ntpath
import git
from pathlib import Path
from typing import List
from git import RemoteReference
from git.repo import Repo
from git.remote import Remote
from hedra.core.graphs.stages.stage import Stage
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
        self.branch = git.Head(self.repo, self.config.path)
        self.git = self.repo.git

    def _checkout(self):

        if self.branch in self.repo.branches:
            self.branch.checkout()

        else:
            self.branch = self.repo.create_head(self.config.branch)

        self.repo.head.reference = self.branch

        remote_reference = RemoteReference(
            self.repo, 
            f"refs/remotes/{self.config.remote}/{self.branch.name}"
        )

        self.repo.head.reference.set_tracking_branch(remote_reference).checkout()

        self.remote.push(self.config.branch)

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

            spec.loader.exec_module(module)
            
            direct_decendants = list({cls.__name__: cls for cls in Stage.__subclasses__()}.values())

            if len(direct_decendants) > 0:                
                graph_files.append(candidate_graph_file_path)

        
        return graph_files

