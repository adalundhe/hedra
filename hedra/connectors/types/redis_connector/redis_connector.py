from __future__ import annotations
from .models import Connection


class RedisConnector:

    def __init__(self, config):
        self.config = config
        self.connection = Connection(self.config)

    async def connect(self) -> RedisConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> RedisConnector:
        return self

    async def execute(self, query, finalize=False) -> RedisConnector:

        await self.connection.execute(query)
        if finalize:
            await self.connection.commit()

        return self

    async def commit(self) -> list:
        return await self.connection.commit()

    async def clear(self) -> RedisConnector:
        await self.connection.clear()
        return self

    async def close(self) -> RedisConnector:
        await self.connection.close()
        return self

