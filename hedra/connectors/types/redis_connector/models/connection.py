import os
import aioredis
import redis
from .types import (
    Channel,
    Pipeline
)
from .statement import Statement



class Connection:

    def __init__(self, config):
        self.config = config
        self.connection = None
        self.options = config.get('options', {})

        self.redis_host = self.config.get(
            'redis_host', 
            os.getenv('REDIS_HOST', 'localhost')
        )

        self.redis_port = int(
            self.config.get(
                'redis_port', 
                os.getenv('REDIS_PORT', 6379)
            )
        )

        self.redis_uri = f'redis://{self.redis_host}:{self.redis_port}'
        self.redis_connection = None

        self._auth = self.config.get('auth', {})
        self.redis_db = int(
            self.config.get(
                'redis_db', 
                os.getenv('REDIS_DB', 0)
            )
        )

    async def connect(self) -> None:

        self.redis_connection = await aioredis.from_url(
            self.redis_uri,
            username=self._auth.get('redis_username'),
            password=self._auth.get('redis_password'),
            db=self.redis_db
        )

        if self.options.get('as_pubsub'):
            self.connection = Channel(
                self.config,
                self.redis_connection
            )

        else:
            self.connection = Pipeline(
                self.config,
                self.redis_connection
            )

        await self.connection.connect()


    async def execute(self, query) -> None:
        statement = Statement(query)
        await self.connection.execute(statement)

    async def commit(self) -> list:
        return await self.connection.commit()

    async def clear(self) -> None:
        await self.connection.clear()

    async def close(self) -> None:
        await self.connection.close()