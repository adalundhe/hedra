from async_tools.tools import Env
from async_tools.datatypes import AsyncDict


class ConnectionConfig:

    def __init__(self, config) -> None:

        if config is None:
            config = {}

        self.config = AsyncDict()
        self._env = Env(config)
        self.postgres_host = None
        self.postgres_port = None
        self.postgres_db = None

        self.auth = AsyncDict()
        self.has_auth = True
        self.postgres_user = None
        self.postgres_password = None

    async def parse_connection_config(self):
        self.postgres_host = await self.config.get(
            'postgres_host',
            await self._env.load_envar('PGHOST', 'localhost')
        )
        
        self.postgres_port = await self.config.get(
            'postgres_port',
            await self._env.load_envar('PGPORT', 5432)
        )

        self.postgres_db = await self.config.get(
            'postgres_db',
            await self._env.load_envar('PGDATABASE', 'postgres')
        )

        await self.auth.update(
            await self.config.get('auth', {})
        )

        self.postgres_user = await self.auth.get(
            'postgres_user',
            await self._env.load_envar('PGUSER', 'postgres')
        )
        self.postgres_password = await self.auth.get(
            'postgres_password',
            await self._env.load_envar('PGPASSWORD', "")
        )

        if self.postgres_password == "":
            self.has_auth = False

        