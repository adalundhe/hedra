class RepoConfig:

    def __init__(self, 
        path: str, 
        uri: str, 
        branch: str='main',
        remote: str='origin', 
        sync_message: str=None,
        username: str=None, 
        password: str=None
    ) -> None:

        self.path = path
        self.uri = uri
        self.branch = branch
        self.remote = remote
        self.sync_message = sync_message
        self.username = username
        self.password = password