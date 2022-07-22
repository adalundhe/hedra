from __future__ import annotations
from .models import Connection


class StatStreamConnector:

    def __init__(self, config):
        self.config = config
        self.connection = Connection(self.config)

    async def connect(self) -> StatStreamConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> StatStreamConnector:
        return self

    async def execute(self, query) -> StatStreamConnector:
        await self.connection.execute(query)
        return self

    async def commit(self) -> list:
        return await self.connection.commit()

    async def clear(self) -> StatStreamConnector:
        await self.connection.clear()
        return self

    async def close(self) -> StatStreamConnector:
        await self.connection.close()
        return self
