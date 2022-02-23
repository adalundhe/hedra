import os

class ConnectionConfig:

    def __init__(self, config) -> None:

        if config is None:
            config = {}

        self.config = config
        self.postgres_host = None
        self.postgres_port = None
        self.postgres_db = None

        self.auth = None
        self.has_auth = True
        self.postgres_user = None
        self.postgres_password = None

    def parse_connection_config(self):

        self.postgres_host = self.config.get(
            'postgres_host',
            os.getenv('POSTGRES_HOST')
        )
        
        self.postgres_port = self.config.get(
            'postgres_port',
            os.getenv('POSTGRES_PORT')
        )

        self.postgres_db = self.config.get(
            'postgres_db',
            os.getenv('POSTGRES_DB', 'postgres')
        )

        self.auth = self.config.get('auth', {})

        self.postgres_user = self.auth.get(
            'postgres_user',
            os.getenv('POSTGRES_USER', 'postgres')
        )
        self.postgres_password = self.auth.get(
            'postgres_password',
            os.getenv('POSTGRES_PASSWORD', "")
        )

        if self.postgres_password == "":
            self.has_auth = False