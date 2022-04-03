from __future__ import annotations
from .models import Connection


class StatServeConnector:

    def __init__(self, config):
        self.config = config
        self.connection = Connection(self.config)

    async def connect(self) -> StatServeConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> StatServeConnector:
        return self

    async def execute(self, query) -> StatServeConnector:
        await self.connection.execute(query)
        return self

    async def execute_stream(self, queries):
        async for result in self.connection.execute_stream(queries):
            yield result

    async def commit(self) -> list:
        return await self.connection.commit()

    async def clear(self) -> StatServeConnector:
        await self.connection.clear()
        return self

    async def close(self) -> StatServeConnector:
        await self.connection.close()
        return self