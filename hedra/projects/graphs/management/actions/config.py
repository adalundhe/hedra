from typing import Optional

class RepoConfig:

    def __init__(self, 
        path: str, 
        uri: str, 
        branch: str='main',
        remote: str='origin', 
        sync_message: Optional[str]=None,
        username: Optional[str]=None, 
        password: Optional[str]=None,
        ignore_options: Optional[str]=None
    ) -> None:

        self.path = path
        self.uri = uri
        self.branch = branch
        self.remote = remote
        self.sync_message = sync_message
        self.username = username
        self.password = password
        self.ignore_options = [
            '__pycache__',
            '**/*.pyx'
        ]

        if isinstance(ignore_options, str) and len(ignore_options) > 0:
            self.ignore_options.extend(
                ignore_options.split(',')
            )

