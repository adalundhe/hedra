from __future__ import annotations
import pymongo
from .models import Connection


class MongoDBConnector:

    def __init__(self, config):
        self.format = 'mongodb'
        self.config = config
        self.connection = Connection(self.config)

    async def connect(self) -> MongoDBConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> MongoDBConnector:
        pass
        return self

    async def execute(self, query) -> MongoDBConnector:
        await self.connection.execute(query)
        return self

    async def commit (self) -> list:
        return await self.connection.commit()

    async def clear(self) -> MongoDBConnector:
        await self.connection.clear()
        return self

    async def close(self) -> MongoDBConnector:
        await self.connection.close()
        return self

