from __future__ import annotations
import os
from motor.motor_asyncio import AsyncIOMotorClient
from async_tools.functions import awaitable
from .database import Database
from .query import Query


class Connection:

    def __init__(self, config):
        self.config = config
        self.client = None
        self.database = None

    async def connect(self) -> Connection:
        mongodb_host=self.config.get(
            'mongodb_host',
            os.getenv('MONGO_DB_HOST', 'localhost')
        )

        mongodb_port=self.config.get(
            'mongodb_port',
            os.getenv('MONGODB_PORT', 27017)
        )


        if self.config.get('mongodb_auth'):
            auth = self.client.get('mongodb_auth')

            username=auth.get(
                'mongodb_user',
                os.getenv('MONGO_DB_USER')
            )

            password=auth.get(
                'mongodb_password',
                os.getenv('MONGOD_DB_PASSWORD')
            )

            mongodb_uri = f'mongodb://{username}:{password}@{mongodb_host}:{mongodb_port}'

        else:
            mongodb_uri = f'mongodb://{mongodb_host}:{mongodb_port}'

        self.client = AsyncIOMotorClient(
            mongodb_uri,
            tz_aware=self.config.get('mongodb_tz_aware', False)
        )

        return self

    async def execute(self, query) -> Connection:
        query = Query(query)
        self.database = Database(self.client, self.config)
        await self.database.execute(query)
        return self

    async def commit(self) -> Connection:
        return await self.database.commit()

    async def clear(self) -> Connection:
        self.database = Database(self.client, self.config)
        await self.database.clear()
        return self

    async def close(self) -> Connection:
        await awaitable(self.client.close)
        return self

    
   